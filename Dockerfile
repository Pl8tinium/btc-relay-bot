# Use the official Node.js 20 image as a parent image
FROM node:20

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the package.json and package-lock.json (if available)
COPY package*.json ./

# Install any dependencies
RUN npm install

# Copy the rest of the application's source code from the local directory to the container
COPY . .

# Compile TypeScript (index.mts) to JavaScript
RUN npm install -g typescript
RUN tsc


# Run the compiled JavaScript file
CMD ["node", "index.js"]