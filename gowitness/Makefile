all: run
run: build
	POSTGRES_HOST=globalwitness.cxl4vro2ssou.us-east-1.rds.amazonaws.com \
POSTGRES_PASS=yvpd7zt82kMk1g3Co92PVKRGjHF2FYEUi0rta \
POSTGRES_DB=globalwitness \
POSTGRES_USER=globalwitness \
MAX_PEERS=120000 ./globalwitness

build: clean
	go build

clean:
	rm -rvf globalwitness
