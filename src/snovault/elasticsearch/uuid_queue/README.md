# Installing redis manually in ubuntu home
## https://redis.io/download
* $mkdir ~/manual-redis && cd manual-redis
* $wget http://download.redis.io/releases/redis-4.0.9.tar.gz
* $tar xzf redis-4.0.9.tar.gz
* $cd redis-4.0.9
* $make
* $src/redis-server
* ctrl-c to exit redis-server

# Configure redis for remote access - Setup consider it's running on a private network
## https://hostpresto.com/community/tutorials/how-to-install-and-configure-redis-on-ubuntu-14-04/
* $cd manual-redis
* $cp redis.conf redis.conf.default
* $echo 'bind 0.0.0.0' >> redis.conf
And change the port from 6379 to 9300
* $vi ~/start-redis-server.sh
    * Add the following two lines
#!/bin/bash
~/manual-redis/redis-4.0.9/src/redis-server ~/manual-redis/redis-4.0.9/redis.conf 

# Start server on port
* $~/start-redis-server.sh 

# Start client on different machine in network.  Install the same as above
* $~/manual-redis/redis-4.0.9/src/redis-client -h your-ip-add -p 9300
