FROM golang:1.24 AS builder

RUN apt-get update && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -o events .

EXPOSE 8082

CMD ["./events"]
