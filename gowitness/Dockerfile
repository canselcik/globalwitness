FROM golang:latest

RUN mkdir /dist
ADD . /dist/
WORKDIR /dist

RUN go build -o main .

EXPOSE 8080
CMD ["/dist/main"]
