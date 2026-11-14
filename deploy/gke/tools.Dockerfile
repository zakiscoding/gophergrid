# gcloud + kubectl + the GKE auth plugin + terraform in one image, so the laptop needs only Docker.
FROM hashicorp/terraform:1.17.2 AS tf
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine
# the alpine variant installs gcloud from the tarball, so `gcloud components install` works
# (the :stable image is apt-managed and has no Google apt repo configured)
RUN gcloud components install -q kubectl gke-gcloud-auth-plugin \
 && rm -rf /google-cloud-sdk/.install/.backup
COPY --from=tf /bin/terraform /usr/local/bin/terraform
WORKDIR /work
