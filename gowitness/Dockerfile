FROM golang:latest

RUN mkdir /dist
ADD . /dist/
WORKDIR /dist

RUN go build -o main .

ENV POSTGRES_HOST=35.225.68.103
ENV POSTGRES_PASS=d7zt82kMk1g3Co92PVKRG
ENV POSTGRES_DB=globalwitness
ENV POSTGRES_USER=postgres
ENV MAX_PEERS=1200

EXPOSE 8080
CMD ["/dist/main"]
