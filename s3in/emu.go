package s3in

//Code modified from github.com/johannesboyne/gofakes3
import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/xiaokangwang/s3emu/lgpd"
)

type GoFakeS3 struct {
	access       map[string]lgpd.LGPD
	timeLocation *time.Location
}
type Storage struct {
	XMLName     xml.Name     `xml:"ListAllMyBucketsResult"`
	Xmlns       string       `xml:"xmlns,attr"`
	Id          string       `xml:"Owner>ID"`
	DisplayName string       `xml:"Owner>DisplayName"`
	Buckets     []BucketInfo `xml:"Buckets"`
}
type BucketInfo struct {
	Name         string `xml:"Bucket>Name"`
	CreationDate string `xml:"Bucket>CreationDate"`
}
type Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}
type Bucket struct {
	XMLName  xml.Name   `xml:"ListBucketResult"`
	Xmlns    string     `xml:"xmlns,attr"`
	Name     string     `xml:"Name"`
	Prefix   string     `xml:"Prefix"`
	Marker   string     `xml:"Marker"`
	Contents []*Content `xml:"Contents"`
}
type Object struct {
	Metadata map[string]string
	Obj      []byte
}

// Setup a new fake object storage
func New() *GoFakeS3 {

	log.Println("locals3 db created or opened")

	timeLocation, err := time.LoadLocation("GMT")
	if err != nil {
		log.Fatal(err)
	}

	return &GoFakeS3{timeLocation: timeLocation}
}

// Create the AWS S3 API
func (g *GoFakeS3) Server() http.Handler {
	r := mux.NewRouter()
	r.Queries("marker", "prefix")
	// BUCKET
	r.HandleFunc("/", g.GetBuckets).Methods("GET")
	r.HandleFunc("/{BucketName}", g.GetBucket).Methods("GET")
	r.HandleFunc("/{BucketName}", g.CreateBucket).Methods("PUT")
	r.HandleFunc("/{BucketName}", g.DeleteBucket).Methods("DELETE")
	r.HandleFunc("/{BucketName}", g.HeadBucket).Methods("HEAD")
	// OBJECT
	r.HandleFunc("/{BucketName}/", g.CreateObjectBrowserUpload).Methods("POST")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.GetObject).Methods("GET")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.CreateObject).Methods("PUT")
	r.HandleFunc("/{BucketName}/{ObjectName:.{0,}}", g.CreateObject).Methods("POST")
	r.HandleFunc("/{BucketName}/{ObjectName:.{1,}}", g.DeleteObject).Methods("DELETE")
	r.HandleFunc("/{BucketName}/{ObjectName:.{0,}}", g.HeadObject).Methods("HEAD")

	return &WithCORS{r}
}

type WithCORS struct {
	r *mux.Router
}

func (s *WithCORS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, HEAD")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-Amz-User-Agent, X-Amz-Date, x-amz-meta-from, x-amz-meta-to, x-amz-meta-filename, x-amz-meta-private")
	w.Header().Set("Content-Type", "application/xml")

	if r.Method == "OPTIONS" {
		return
	}
	// Bucket name rewriting
	// this is due to some inconsistencies in the AWS SDKs
	re := regexp.MustCompile("(127.0.0.1:\\d{1,7})|(.localhost:\\d{1,7})|(localhost:\\d{1,7})")
	bucket := re.ReplaceAllString(r.Host, "")
	if len(bucket) > 0 {
		log.Println("rewrite bucket ->", bucket)
		p := r.URL.Path
		r.URL.Path = "/" + bucket
		if p != "/" {
			r.URL.Path += p
		}
	}
	log.Println("=>", r.URL)

	s.r.ServeHTTP(w, r)
}

// Get a list of all Buckets
func (g *GoFakeS3) GetBuckets(w http.ResponseWriter, r *http.Request) {
	var buckets []BucketInfo
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>`))

	for bucketname := range g.access {
		buckets = append(buckets, BucketInfo{string(bucketname), ""})
	}

	s := &Storage{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Id:          "fe7272ea58be830e56fe1663b10fafef",
		DisplayName: "GoFakeS3",
		Buckets:     buckets,
	}
	x, err := xml.MarshalIndent(s, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Write(x)
}

// GetBucket lists the contents of a bucket.
func (g *GoFakeS3) GetBucket(w http.ResponseWriter, r *http.Request) {
	log.Println("GET BUCKET")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]

	log.Println("bucketname:", bucketName)
	log.Println("prefix    :", r.URL.Query().Get("prefix"))

	access, ok := g.access[bucketName]

	if !ok {
		http.Error(w, "No bucket", http.StatusNotFound)
		return
	}

	bucketc := &Bucket{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:     "crowdpatent.com",
		Prefix:   r.URL.Query().Get("prefix"),
		Marker:   "",
		Contents: []*Content{},
	}

	list := access.List(r.URL.Query().Get("prefix"))

	for _, filec := range list {
		if strings.Contains(filec.Name, r.URL.Query().Get("prefix")) {
			bucketc.Contents = append(bucketc.Contents, &Content{
				Key:          filec.Name,
				LastModified: g.timeNow().Format(time.RFC3339),
				ETag:         "\"" + filec.Mark + "\"",
				Size:         filec.Length,
				StorageClass: "STANDARD",
			})

		}
	}
	x, err := xml.MarshalIndent(bucketc, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(x)

}

// CreateBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) CreateBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("CREATE BUCKET:", bucketName)

	//We don't support this
	http.Error(w, "bucket existed", http.StatusBadRequest)
	return

}

// DeleteBucket creates a new S3 bucket in the BoltDB storage.
func (g *GoFakeS3) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "delete bucket")
}

// HeadBucket checks whether a bucket exists.
func (g *GoFakeS3) HeadBucket(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("HEAD BUCKET", bucketName)
	log.Println("bucketname:", bucketName)

	_, ok := g.access[bucketName]

	if !ok {
		http.Error(w, "bucket does not exist", http.StatusNotFound)
		return
	}
}

// GetObject retrievs a bucket object.
func (g *GoFakeS3) GetObject(w http.ResponseWriter, r *http.Request) {
	log.Println("GET OBJECT")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	access, ok := g.access[bucketName]

	if !ok {
		log.Println("no bucket")
		http.Error(w, "bucket does not exist", http.StatusNotFound)
		return
	}

	data, meta, err := access.Get(vars["ObjectName"], false)

	if err != nil {
		log.Println("can't get")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")

	w.Header().Set("Last-Modified", g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST"))
	w.Header().Set("ETag", "\""+meta.Mark+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", meta.Length))
	w.Header().Set("Connection", "close")
	w.Write(data)

}

// CreateObject (Browser Upload) creates a new S3 object.
func (g *GoFakeS3) CreateObjectBrowserUpload(w http.ResponseWriter, r *http.Request) {
	log.Println("CREATE OBJECT THROUGH BROWSER UPLOAD")
	const _24K = (1 << 20) * 24
	if err := r.ParseMultipartForm(_24K); nil != err {
		panic(err)
	}
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	key := r.MultipartForm.Value["key"][0]

	log.Println("(BUC)", bucketName)
	log.Println("(KEY)", key)
	fileHeader := r.MultipartForm.File["file"][0]
	infile, err := fileHeader.Open()
	if nil != err {
		panic(err)
	}
	body, err := ioutil.ReadAll(infile)
	if err != nil {
		panic(err)
	}

	access, ok := g.access[bucketName]

	if !ok {
		log.Println("no bucket")
		http.Error(w, "bucket does not exist", http.StatusNotFound)
		return
	}

	meta := make(map[string]string)
	log.Println(r.MultipartForm)
	for hk, hv := range r.MultipartForm.Value {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST")

	err = access.Put(key, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		fmt.Errorf("error while creating")
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})

}

// CreateObject creates a new S3 object.
func (g *GoFakeS3) CreateObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]

	log.Println("CREATE OBJECT:", bucketName, vars["ObjectName"])
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	access, ok := g.access[bucketName]

	if !ok {
		log.Println("no bucket")
		http.Error(w, "bucket does not exist", http.StatusNotFound)
		return
	}

	meta := make(map[string]string)
	for hk, hv := range r.Header {
		if strings.Contains(hk, "X-Amz-") {
			meta[hk] = hv[0]
		}
	}
	meta["Last-Modified"] = g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST")
	key := vars["ObjectName"]

	err = access.Put(key, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		fmt.Errorf("error while creating")
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")
	w.Header().Set("ETag", "\"fbacf535f27731c9771645a39863328\"")
	w.Header().Set("Server", "AmazonS3")
	w.Write([]byte{})
}

// DeleteObject deletes a S3 object from the bucket.
func (g *GoFakeS3) DeleteObject(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "delete object")
}

// HeadObject retrieves only meta information of an object and not the whole.
func (g *GoFakeS3) HeadObject(w http.ResponseWriter, r *http.Request) {
	log.Println("HEAD OBJECT")
	vars := mux.Vars(r)
	bucketName := vars["BucketName"]
	log.Println("Bucket:", bucketName)
	log.Println("└── Object:", vars["ObjectName"])

	access, ok := g.access[bucketName]

	if !ok {
		log.Println("no bucket")
		http.Error(w, "bucket does not exist", http.StatusNotFound)
		return
	}

	data, meta, err := access.Get(vars["ObjectName"], true)

	if err != nil {
		log.Println("can't get")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-amz-id-2", "LriYPLdmOdAiIfgSm/F1YsViT1LW94/xUQxMsF7xiEb1a0wiIOIxl+zbwZ163pt7")
	w.Header().Set("x-amz-request-id", "0A49CE4060975EAC")

	w.Header().Set("Last-Modified", g.timeNow().Format("Mon, 2 Jan 2006 15:04:05 MST"))
	w.Header().Set("ETag", "\""+meta.Mark+"\"")
	w.Header().Set("Server", "AmazonS3")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", meta.Length))
	w.Header().Set("Connection", "close")
	w.Write(data)

}

func (g *GoFakeS3) timeNow() time.Time {
	return time.Now().In(g.timeLocation)
}

func (g *GoFakeS3) SetSource(bk string, hd lgpd.LGPD) {
	if g.access == nil {
		g.access = make(map[string]lgpd.LGPD)
	}
	g.access[bk] = hd
}
