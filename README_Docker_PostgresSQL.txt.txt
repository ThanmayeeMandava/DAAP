This document provides instructions on how to set up and run MongoDB and PostgreSQL using Docker containers.

Docker Setup Instructions:
--------------------------
1. **Open Command Prompt/Terminal**:
   - On Windows, open Command Prompt. On Mac, open Terminal.

2. **Navigate to Project Directory**:
   - Change to the directory where the `docker-compose.yml` file is located.

3. **Build and Run Docker Containers**:
   - Execute the following command to create and start the Docker containers:
     ```
     docker-compose -p group_o up --build -d
     ```
   - This command builds the Docker images if they don't already exist and starts the containers in detached mode.

4. **Verify Container Status**:
   - Ensure that the Docker containers for MongoDB and PostgreSQL are running. You can check the status of the containers with:
     ```
     docker ps
     ```

5. **Database Configuration**:
   - Once the containers are up, update your application's database configuration 
      to connect to these Docker-managed databases.

PostgreSQL Instruction:
-----------------------

1. Create one New Database in PostgreSQL
