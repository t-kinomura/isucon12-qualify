deploy:
	echo "nothing to do"

bench-prepare:
	sudo rm -f /var/log/mysql/mysql-slow.log
	sudo systemctl restart mysql.service

show-slowlog:
	sudo mysqldumpslow /var/log/mysql/mysql-slow.log

