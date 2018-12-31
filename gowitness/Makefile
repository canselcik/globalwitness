IMAGE_NAME=globalwitness
RELEASE=latest
REPO=376692230222.dkr.ecr.us-east-2.amazonaws.com

all: build

docker:
	docker build -t $(IMAGE_NAME) .

release: docker
	docker tag $(IMAGE_NAME):$(RELEASE) $(REPO)/$(IMAGE_NAME):$(RELEASE)
	docker push $(REPO)/$(IMAGE_NAME):$(RELEASE)

build: clean
	go build

clean:
	rm -rvf globalwitness
