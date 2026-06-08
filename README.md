# natfy

Fastly publisher for `github.com/csbxd/natnet`.

`Client` implements `natnet.Syncer`. When the public address changes it checks
the active service version backend, skips work if the backend already matches,
otherwise clones the active version, updates the backend address and port,
validates the clone, and activates it.

```go
fy := natfy.New(natfy.Config{
	APIKey:      os.Getenv("FASTLY_API_TOKEN"),
	ServiceID:   "...",
	BackendName: "origin",
})

r, err := natnet.Start(ctx, natnet.Config{
	LocalAddr: &net.TCPAddr{Port: 26656},
	Syncer:    fy,
})
```
