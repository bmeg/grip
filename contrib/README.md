
# Adding GRIP as a service

## Ubuntu

Add to service control
```
sudo systemctl enable /full/path/to/grip.service
sudo systemctl daemon-reload
```

Restart service
```
sudo systemctl restart grip
```

View logs

```
journalctl -u grip
```
