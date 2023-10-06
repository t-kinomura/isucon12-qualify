deploy:
	cp -r ./webapp/go ../webapp
	sudo systemctl restart isuports.service

bench-prepare:
	sudo rm -f /var/log/nginx/access.log
	sudo systemctl reload nginx.service
	time mysql -uroot -proot isuports < ~/dump/initial_data_dump.txt
	sudo rm -f /var/log/mysql/mysql-slow.log
	sudo systemctl restart mysql.service

bench-result:
	mkdir -p alp/dump
	cat /var/log/nginx/access.log \
	| alp ltsv \
	-m '/api/organizer/player/[0-9a-z]+/disqualified$$,/api/organizer/competition/[0-9a-z]+/finish$$,/api/organizer/competition/[0-9a-z]+/score$$,/api/player/competition/[0-9a-z]+/ranking$$,/api/player/player/[0-9a-z]+$$' \
	--sort avg -r --dump alp/dump/`git show --format='%h' --no-patch` > /dev/null

latest-alp:
	mkdir -p alp/result
	alp ltsv --load alp/dump/`git show --format='%h' --no-patch` > alp/result/`git show --format='%h' --no-patch`
	vim alp/result/`git show --format='%h' --no-patch`

show-slowlog:
	sudo mysqldumpslow /var/log/mysql/mysql-slow.log

show-applog:
	make -C webapp/go show-applog

enable-pprof:
	sed -i -e 's/PPROF=0/PPROF=1/' env.sh

disable-pprof:
	sed -i -e 's/PPROF=1/PPROF=0/' env.sh

start-pprof: enable-pprof deploy
	go tool pprof -http=0.0.0.0:1080 ~/webapp/go/isucholar http://localhost:6060/debug/pprof/profile?seconds=80

