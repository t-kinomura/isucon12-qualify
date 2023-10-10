deploy:
	cp -r ./webapp/go ../webapp
	cp ./webapp/docker-compose-go.yml ../webapp
	cp ./webapp/sql/init.sh ../webapp/sql/init.sh
	cp ./webapp/sql/init.sql ../webapp/sql/init.sql
	sudo systemctl restart isuports.service

bench-prepare:
	echo "noting to do"

bench-result:
	mkdir -p alp/dump
	cat /var/log/nginx/access.log \
	| alp ltsv \
	-m '/api/organizer/player/[0-9a-zA-Z\-]+/disqualified$$,/api/organizer/competition/[0-9a-zA-Z\-]+/finish$$,/api/organizer/competition/[0-9a-zA-Z\-]+/score$$,/api/player/competition/[0-9a-zA-Z\-]+/ranking$$,/api/player/player/[0-9a-zA-Z\-]+$$' \
	--sort avg -r --dump alp/dump/`git show --format='%h' --no-patch` > /dev/null

latest-alp:
	mkdir -p alp/result
	alp ltsv --load alp/dump/`git show --format='%h' --no-patch` > alp/result/`git show --format='%h' --no-patch`
	vim alp/result/`git show --format='%h' --no-patch`

show-slowlog:
	echo "2台目で実行してください"

show-applog:
	make -C webapp/go show-applog

enable-pprof:
	sed -i -e 's/PPROF: 0/PPROF: 1/' ./webapp/docker-compose-go.yml

disable-pprof:
	sed -i -e 's/PPROF: 1/PPROF: 0/' ./webapp/docker-compose-go.yml

start-pprof: enable-pprof deploy
	go tool pprof -http=0.0.0.0:1080 http://localhost:6060/debug/pprof/profile?seconds=100

