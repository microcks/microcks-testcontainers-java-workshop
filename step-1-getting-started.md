# Step 1: Getting Started

Before getting started, let's make sure you have everything you need for running this demo.

## Prerequisites

### Install Java 17 or newer

You'll need Java 17 or newer for this workshop.
Testcontainers libraries are compatible with Java 8+, but this workshop uses a Spring Boot 3.x application which requires Java 17 or newer.

We would recommend using [SDKMAN](https://sdkman.io/) to install Java on your machine if you are using MacOS, Linux or Windows WSL.

### Install Docker

You need to have a [Docker](https://docs.docker.com/get-docker/) or [Podman](https://podman.io/) environment to use Testcontainers.

```shell
$ docker version

Client:
 Version:           29.4.2
 API version:       1.54
 Go version:        go1.26.2
 Git commit:        055a478
 Built:             Fri May  1 01:23:38 2026
 OS/Arch:           darwin/arm64
 Context:           desktop-linux

Server: Docker Desktop 4.72.0 (225998)
 Engine:
  Version:          29.4.2
  API version:      1.54 (minimum version 1.40)
  Go version:       go1.26.2
  Git commit:       d329809
  Built:            Fri May  1 01:25:57 2026
  OS/Arch:          linux/arm64
  Experimental:     false
 containerd:
  Version:          v2.2.3
  GitCommit:        77c84241c7cbdd9b4eca2591793e3d4f4317c590
 runc:
  Version:          1.3.5
  GitCommit:        v1.3.5-0-g488fc13e
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

## Download the project

Clone the [microcks-testcontainers-java-workshop](https://github.com/microcks/microcks-testcontainers-java-workshop) repository from GitHub to your computer:

```shell
git clone https://github.com/microcks/microcks-testcontainers-java-workshop.git
```

## Compile the project to download the dependencies

With Maven:

```shell
./mvnw clean package -DskipTests
```

### 

[Next](step-2-exploring-the-app.md)