package gdrive

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/xiaokangwang/s3emu/lgpd"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

type GDriveBackend struct {
	srv          *drive.Service
	uploadprefix string
}

func NewGDriveBackend(prefix string) *GDriveBackend {
	return &GDriveBackend{uploadprefix: prefix}
}

func (ntq *GDriveBackend) ensureToken() {
	b, err := ioutil.ReadFile("credentials.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	// If modifying these scopes, delete your previously saved client_secret.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}

	srv, err := drive.New(getClient(config))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	ntq.srv = srv
}

// Retrieve a token, saves the token, then returns the generated client.
func getClient(config *oauth2.Config) *http.Client {
	tokenFile := "token.json"
	tok, err := tokenFromFile(tokenFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokenFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	defer f.Close()
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	json.NewEncoder(f).Encode(token)
}

func (ntq *GDriveBackend) Put(key string, value []byte) error {
	ntq.ensureToken()
	var err error
	var file drive.File
	file.Name = key
	file.Parents = []string{ntq.uploadprefix}
EnqueueUploadTask_retry:
	_, err = ntq.srv.Files.Create(&file).Media(bytes.NewReader(value)).Do()
	if err != nil {
		fmt.Println(err)
		goto EnqueueUploadTask_retry
	}
	return nil
}
func (ntq *GDriveBackend) Get(key string, nofetch bool) ([]byte, lgpd.File, error) {
	ntq.ensureToken()
	var ret lgpd.File
	ret.Name = key
	fn := key
	r, err := ntq.srv.Files.List().Q("name = '" + fn + "' and '" + ntq.uploadprefix + "' in parents ").PageSize(10).
		Fields("nextPageToken, files(*)").Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}

	if len(r.Files) == 0 {
		return nil, ret, errors.New("File not found")
	}

	did := r.Files[0].Id

	ret.Length = int(r.Files[0].Size)
	ret.Mark = r.Files[0].Md5Checksum

	if nofetch {
		return nil, ret, nil
	}

	abuseFlag := false
EnqueueDownloadTask_download:
	fd := ntq.srv.Files.Get(did)
	resp, err := fd.AcknowledgeAbuse(abuseFlag).Download()
	if err != nil {
		if !abuseFlag {
			abuseFlag = true
			goto EnqueueDownloadTask_download
		}
		goto EnqueueDownloadTask_download
	}
	c, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		goto EnqueueDownloadTask_download
	}
	resp.Body.Close()

	return c, ret, nil
}

func (ntq *GDriveBackend) List(perfix string) []lgpd.File {
	ntq.ensureToken()
	var ret []lgpd.File
	var nextpageToken string

FetchPage:
	r, err := ntq.srv.Files.List().Q("'" + ntq.uploadprefix + "' in parents ").PageToken(nextpageToken).PageSize(1000).
		Fields("nextPageToken, files(*)").Do()
	if err != nil {
		log.Fatalf("Unable to retrieve files: %v", err)
	}
	nextpageToken = r.NextPageToken

	for _, i := range r.Files {
		var retx lgpd.File
		retx.Name = i.Name
		retx.Length = int(i.Size)
		retx.Mark = i.Md5Checksum
		ret = append(ret, retx)
	}

	if nextpageToken != "" {
		goto FetchPage
	}
	return ret
}
