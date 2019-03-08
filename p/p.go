package p

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"cloud.google.com/go/storage"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

func void() {}

func getCredentials() (string, string) {
	return os.Getenv("SFTP_USERNAME"), os.Getenv("SFTP_PASSWORD")
}
func getHost() string {
	return fmt.Sprintf("%s:%s", os.Getenv("SFTP_HOST"), os.Getenv("SFTP_PORT"))
}
func getSFTPPrefixPath() string {
	return os.Getenv("SFTP_PREFIX_PATH")
}
func getSFTPPath(key string) string {
	return fmt.Sprintf("%s/%s", getSFTPPrefixPath(), key)
}

func getFile(bucketName, key string) (io.ReadCloser, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	bucket := client.Bucket(bucketName)
	object := bucket.Object(key)
	file, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func pushToSFTP(r io.Reader, path string) error {
	user, pass := getCredentials()
	config := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(pass),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	conn, err := ssh.Dial("tcp", getHost(), config)
	if err != nil {
		log.Fatalf("unable to connect: %s", err)
	}
	defer conn.Close()

	sftp, err := sftp.NewClient(conn)
	if err != nil {
		return err
	}
	defer sftp.Close()

	f, err := sftp.Create(path)
	if err != nil {
		return err
	}
	io.Copy(f, r)
	defer f.Close()

	return nil
}

func SyncFile(ctx context.Context, e GCSEvent) error {
	log.Printf("Processing file: gs://%s/%s", e.Bucket, e.Name)
	f, err := getFile(e.Bucket, e.Name)
	if err != nil {
		return err
	}
	defer f.Close()
	log.Printf("Push to sftp://%s %s", getHost(), getSFTPPath(e.Name))
	if err := pushToSFTP(f, getSFTPPath(e.Name)); err != nil {
		return err
	}
	return nil
}
