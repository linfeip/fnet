module github.com/linfeip/fnet/examples

go 1.23.0

require (
	github.com/gobwas/ws v1.4.0
	github.com/linfeip/fnet v0.0.0
	github.com/valyala/fasthttp v1.58.0
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
)

// The examples run against the fnet in this repository.
replace github.com/linfeip/fnet => ../
