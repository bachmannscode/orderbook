# Order Book App

## Prerequisites
- Docker installed on your system.

## Building the Docker Image
1. Clone this repository:
   ```sh
   git clone https://github.com/bachmannscode/orderbook
   ```
2. Navigate to the project directory:
   ```sh
   cd orderbook
   ```
3. Build the Docker image:
   ```sh
   docker build -t orderbook .
   ```

## Running the Container

### 1. Run the container with the default config:
```sh
   docker run -it orderbook
```

### 2. (Optional) Mount a custom config file:
If you want to use a different trading pair or print interval, you can modify the `config.toml` file and mount it at runtime:
```sh
docker run -itv "$(pwd)/config.toml:/app/config.toml" orderbook
```

## Stopping the Container
If running interactively (`-it`), press:
```sh
Ctrl + C
```
If the app does not respond, open a new terminal and run:
```sh
docker ps   # Find the container ID

docker stop <container_id>
```
Or force kill it if necessary:
```sh
docker kill <container_id>
```

