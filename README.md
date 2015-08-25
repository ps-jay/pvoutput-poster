```
docker build --tag="local/pvposter" .
docker run -d -m 128m \
    -v=/opt/energy:/data:rw \
    -e API_KEY=(an_api_key) \
    -e SYSTEM_ID=(a_system_id) \
    --name=pvposter local/pvposter \
    sh -c "while [ 1 ] ; do python /opt/pvposter/pvoutput-poster.py ; if [ $? -ne 0 ] ; then break ; fi ; sleep 550 ; done"
```
