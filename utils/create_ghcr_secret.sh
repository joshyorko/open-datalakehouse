#!/bin/bash



create_ghcr_secret() {
  local output_file="ghcr-login-secret.yaml"
  echo -e "${GREEN}🔐 Creating GitHub Container Registry secret YAML...${NC}"

  read -s -p "Enter the token for the docker-password: " token
  echo

  kubectl create secret docker-registry ghcr-login \
    --docker-server=ghcr.io \
    --docker-username=jyorko \
    --docker-password="$token" \
    --docker-email=email@example.com \
    --dry-run=client -o yaml > "$output_file"

  echo -e "${GREEN}🔐 GitHub Container Registry secret YAML file created: $output_file${NC}"
}


# Main script execution
main() {
    echo -e "${GREEN}👋 Welcome to the Kubernetes setup script!${NC}"

    create_ghcr_secret

}

main