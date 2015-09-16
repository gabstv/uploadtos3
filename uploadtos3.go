package main

import (
	"flag"
	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var (
	ak      = ""
	sk      = ""
	region  = "us-east-1"
	bucket  = ""
	once    = sync.Once{}
	reduced = true
	lerrors = make([]string, 0)
)

func initMIME() {
	//TODO: add more mime types than go has
}

func guessMIME(fpath string) string {
	once.Do(initMIME)
	ext := filepath.Ext(fpath)
	mt := mime.TypeByExtension(ext)
	if len(mt) > 0 {
		return mt
	}
	//
	fil, err := os.Open(fpath)
	if err != nil {
		return ""
	}
	defer fil.Close()
	bs := make([]byte, 512)
	fil.Read(bs)
	return http.DetectContentType(bs)
}

func headers(ctype string) map[string][]string {
	customHeaders := make(map[string][]string)
	f0 := "REDUCED_REDUNDANCY"
	if !reduced {
		f0 = "STANDARD"
	}
	customHeaders["x-amz-storage-class"] = []string{f0}
	customHeaders["Content-Type"] = []string{ctype}
	return customHeaders
}

func errori(v string) {
	lerrors = append(lerrors, v)
	print(v)
}

func main() {
	flag.StringVar(&ak, "key", "", "Access Key")
	flag.StringVar(&sk, "secret", "", "Secret Access Key")
	flag.StringVar(&region, "region", "us-east-1", "Region")
	flag.StringVar(&bucket, "bucket", "", "Bucket")
	flag.BoolVar(&reduced, "reduced", true, "Use reduced redundancy storage")
	flag.Parse()
	if ak == "" || sk == "" || region == "" || bucket == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
	auth := aws.NewAuth(ak, sk, "", time.Now().AddDate(0, 0, 15))
	s := s3.New(*auth, aws.Regions[region])
	b := s.Bucket(bucket)

	//b.Put(path, data, contType, s3.PublicRead, opt)
	npaths := flag.NArg()

	for i := 0; i < npaths; i++ {
		p0 := flag.Arg(i)
		fi, err := os.Stat(p0)
		if err != nil {
			errori("Error: " + p0 + " " + err.Error() + "\n")
			continue
		}
		if !fi.IsDir() {
			// upload right away
			ff, err := os.Open(p0)
			if err != nil {
				errori("Error (os.Open): " + p0 + " " + err.Error() + "\n")
				continue
			}
			err = b.PutReaderHeader(filepath.Join("/", p0), ff, fi.Size(), headers(guessMIME(p0)), s3.PublicRead)
			ff.Close()
			if err != nil {
				errori("Error (S3 PUT): " + p0 + " " + err.Error() + "\n")
			}
		} else {
			// clever way to put dir
			filepath.Walk(p0, func(pa string, info os.FileInfo, err error) error {
				print(pa + "\n")
				if err != nil {
					errori("Error: " + err.Error() + "\n")
					return nil
				}
				if info.IsDir() {
					return nil
				}
				//
				ff, err := os.Open(pa)
				if err != nil {
					errori("Error (os.Open): " + pa + " " + err.Error() + "\n")
					return nil
				}
				for t := 0; t < 5; t++ {
					err = b.PutReaderHeader(filepath.Join("/", pa), ff, info.Size(), headers(guessMIME(pa)), s3.PublicRead)
					if err == nil {
						break
					}
					time.Sleep(time.Millisecond * 100)
				}
				ff.Close()
				if err != nil {
					errori("Error (S3 PUT): " + pa + " " + err.Error() + "\n")
				}
				//
				return nil
			})
		}
	}
	if len(lerrors) > 0 {
		print("Errors (" + strconv.Itoa(len(lerrors)) + "):\n")
		for _, v := range lerrors {
			print(v)
		}
	} else {
		print("No errors.\n")
	}
}
