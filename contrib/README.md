
# Adding Arachne as a service

## Ubuntu

Add to service control
```
sudo systemctl enable /full/path/to/arachne.service
sudo systemctl daemon-reload
```

Restart service
```
sudo systemctl restart arachne
```

View logs

```
journalctl -u arachne
```
