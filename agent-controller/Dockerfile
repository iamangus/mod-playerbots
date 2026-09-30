FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agent-controller .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/agent-controller /agent-controller
USER 65532:65532
EXPOSE 8098
ENTRYPOINT ["/agent-controller"]
