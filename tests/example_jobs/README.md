
## Default users:

The igor scheduler process creates default admin user(s) when it starts.

At the moment the process reads and creates users it finds there on start. 
```bash
${IGOR_DEFAULT_ADMIN_USERS}
```
It then nullifies the env var from the env.

Check out docker-compose.yml for details.


Incase it's handy, the token for admin:admin assuming you're just playing around 
and using the current docker-compose file, is
```bash
YWRtaW46YWRtaW4=
```

## Using curl

Since the default transport is HTTPS, you can easily use curl to talk to an igor http gateway. 
The rough format to send a json file is (assuming your user password is admin:admin):

```bash
curl -d @/path/to/file.json -H "Content-Type: application/json" --cacert /path/to/ssl.pem -H "Authorization: Basic YWRtaW46YWRtaW4=" "https://localhost:9025/v1/[url]"
```

