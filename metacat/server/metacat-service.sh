#!/bin/bash

# Default values
action=""
environment="dev"

# Help function to display usage
show_help() {
	echo "Usage: $0 [options]"
	echo "  -a, --action          Action to perform: start, stop, or build"
	echo "  -e, --env             Environment: dev or prod"
	echo "  -h, --help            Display help"
}

# Loop through arguments
while [[ "$#" -gt 0 ]]; do
	case "$1" in
		-a|--action)
			if [[ -n "$2" && "$2" != -* ]]; then
				action="$2"
				shift 2
			else
				echo "Error: --action requires a value (start, stop, or build)."
				exit 1
			fi
			;;
		-e|--env)
			if [[ "$2" == "dev" || "$2" == "prod" ]]; then
				environment="$2"
				shift 2
			else
				echo "Error: --env requires a value (dev or prod)."
				exit 1
			fi
			;;
		-h|--help)
			show_help
			exit 0
			;;
		*)
			echo "Unknown option: $1"
			show_help
			exit 1
			;;
	esac
done

# Check if required parameters are provided
if [[ -z "$action" || -z "$environment" ]]; then
	echo "Error: --action and --env are required."
	show_help
	exit 1
fi

# Set the compose file based on the environment
if [[ "$environment" == "dev" ]]; then
	#compose_file="docker-compose.yml"
	#compose_args="docker-compose.yml"
	compose_args=""
elif [[ "$environment" == "prod" ]]; then
	#compose_file="docker-compose.prod.yml"
	compose_args="-p metacat-prod -f docker-compose.yml -f docker-compose.prod.yml"
fi

# Perform the requested action
case "$action" in
	start)
		echo "Starting all containers in $environment environment"
		eval docker compose "$compose_args" up -d && \
			echo "All containers started successfully in $environment environment." || \
			echo "Failed to start containers in $environment environment."
					;;

	stop)
		echo "Stopping all containers in $environment environment"
		eval docker compose "$compose_args" down && \
			echo "All containers stopped successfully in $environment environment." || \
			echo "Failed to stop containers in $environment environment."
					;;
	build)
		echo "Building all containers in $environment environment"
		#echo docker compose "$compose_args" build
		eval docker compose "$compose_args" build && \
			echo "All containers built successfully in $environment environment." || \
			echo "Failed to build containers in $environment environment."
					;;
	*)
		echo "Error: Invalid action. Available actions are start, stop, and build."
		show_help
		exit 1
		;;
esac

