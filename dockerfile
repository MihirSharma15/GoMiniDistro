# Use the official Golang image as the base image
FROM golang:1.17-alpine

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod (and go.sum if available) files and download dependencies
COPY go.mod ./
RUN go mod download

# Copy the rest of the application code
COPY . .

# Build the Go application
# Build the Go application for Linux
RUN GOOS=linux GOARCH=amd64 go build -o myapp .

ENTRYPOINT ["./myapp"]

# Expose the application port (adjust if your app runs on a different port)
EXPOSE 8080

# Set environment variables (optional)
# ENV PARENT_NODE=<parent_node_address>
# ENV SELF_ADDRESS=<self_address>

# Command to run the executable
CMD ["./main"]