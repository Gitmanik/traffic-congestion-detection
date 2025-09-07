# Build Docker images
build:
	@echo "Building Docker images..."
	docker compose build --no-cache

# Start basic system
up:
	@echo "Starting traffic monitoring system (basic)..."
	docker compose up -d zookeeper kafka schema-registry kafka-ui kafka-setup
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	docker compose up -d traffic-producer spark-detector
	@echo "System started! Check status with: make logs"
	@echo "Kafka UI: http://localhost:8080"
	@echo "Spark UI: http://localhost:4040"

# Clean up everything
clean:
	@echo "Cleaning up containers, images, and volumes..."
	docker compose --profile enhanced down -v --remove-orphans
	docker image prune -f
	docker volume prune -f
	@echo "Cleanup completed!"

# Test the system
test:
	@echo "Running system tests..."
	python test_system.py
