#!/bin/sh
# GopherGrid on GKE, end to end. Runs inside the gophergrid/gke-tools image (gcloud, kubectl,
# terraform) with the laptop's Docker socket for pushing images:
#
#   docker run --rm -it -v gcloud-config:/root/.config/gcloud -v "$PWD:/work" \
#     -v /var/run/docker.sock:/var/run/docker.sock gophergrid/gke-tools:1 sh run.sh <project> <step>
#
# steps: apply | push | deploy | bench | destroy   (each is idempotent; "destroy" deletes everything)
set -eu
PROJECT=$1; STEP=$2; TAG=${TAG:-gke1}; REGION=us-central1; ZONE=us-central1-a
REG=$REGION-docker.pkg.dev/$PROJECT/gophergrid
KEDA=2.21.0
mkdir -p out
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a out/timeline.log; }

case $STEP in
apply)
  terraform init -input=false >/dev/null
  terraform apply -input=false -auto-approve -var project_id=$PROJECT ;;
push)   # needs the docker CLI + socket; images built on the laptop with scripts/build.sh
  gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://$REGION-docker.pkg.dev >/dev/null
  for i in scheduler worker ggbench; do
    docker tag gophergrid/$i:$TAG $REG/$i:$TAG && docker push -q $REG/$i:$TAG
  done ;;
deploy)
  gcloud container clusters get-credentials gophergrid --zone $ZONE --project $PROJECT
  kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v$KEDA/keda-$KEDA.yaml >/dev/null
  kubectl -n keda rollout status deploy/keda-operator --timeout=300s
  kubectl get ns gophergrid >/dev/null 2>&1 || kubectl create namespace gophergrid
  kubectl -n gophergrid get secret gophergrid-db >/dev/null 2>&1 ||
    kubectl -n gophergrid create secret generic gophergrid-db --from-literal=password="$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  cp ../k8s/gophergrid.yaml gophergrid.yaml
  kubectl kustomize . | sed "s#REGISTRY#$REG#g; s#:TAG#:$TAG#g" | kubectl apply -f -
  kubectl -n gophergrid rollout status statefulset/postgres --timeout=300s
  kubectl -n gophergrid rollout status deploy/scheduler --timeout=300s
  kubectl -n gophergrid rollout status deploy/worker --timeout=300s
  log "deployed"; kubectl -n gophergrid get pods -o wide ;;
bench)  # N jobs of MS ms each; KEDA scales workers on queue depth; one worker pod and the
        # scheduler leader pod are deleted mid-run; ggbench checks exactly-once at the end
  N=${N:-3000}; MS=${MS:-500}
  kubectl -n gophergrid delete job ggbench --ignore-not-found >/dev/null
  ( while true; do echo "$(date -u +%H:%M:%S) $(kubectl -n gophergrid get deploy worker -o jsonpath='{.status.replicas}/{.status.readyReplicas}') $(kubectl -n gophergrid get pods -l app=worker --no-headers 2>/dev/null | awk '{print $3}' | sort | uniq -c | tr '\n' ' ')"; sleep 5; done ) > out/workers.log 2>&1 &
  WATCH=$!
  sed "s#REGISTRY#$REG#g; s#:TAG#:$TAG#g; s#N_JOBS#$N#; s#JOB_MS#$MS#" ggbench-job.yaml | kubectl apply -f -
  log "bench start N=$N MS=$MS"
  sleep 60
  W=$(kubectl -n gophergrid get pods -l app=worker -o name | head -1); log "chaos: delete $W"; kubectl -n gophergrid delete $W --wait=false
  sleep 30
  L=$(kubectl -n gophergrid get pods -l app=scheduler -o name | while read p; do
        # the scheduler image is distroless (no shell/wget): ask through the API server's pod proxy
        kubectl get --raw "/api/v1/namespaces/gophergrid/pods/${p#pod/}:8080/proxy/v1/leader" 2>/dev/null |
          grep -q '"is_leader":true' && echo $p; done | head -1)
  log "chaos: delete scheduler leader ${L:-unknown}"; if [ -n "$L" ]; then kubectl -n gophergrid delete $L --wait=false; fi
  kubectl -n gophergrid wait --for=condition=complete job/ggbench --timeout=20m || log "bench job did not complete"
  log "bench done"
  kubectl -n gophergrid logs job/ggbench > out/ggbench.log 2>&1 || true
  kill $WATCH
  kubectl -n gophergrid get events --sort-by=.lastTimestamp > out/events.log
  kubectl -n keda logs deploy/keda-operator --tail=200 > out/keda.log 2>&1 || true ;;
destroy)
  terraform destroy -input=false -auto-approve -var project_id=$PROJECT
  gcloud projects delete $PROJECT --quiet
  log "destroyed cluster + registry (terraform) and deleted project $PROJECT" ;;
esac
