# Airflow in Kubernetes Executor

### Pre-requisite
- Download Docker Desktop from (here)[https://www.docker.com/products/docker-desktop] and setup in your local machine
- Download Virtualbox from (here)[https://www.virtualbox.org/wiki/Downloads] and setup in your local machine
- Install Minikube, Helm & add package repo
    ```bash
          brew update
  
          # install minikube
          brew install minikube
          
          # install helm
          brew install kubernetes-helm
          
          # add package repository to helm 
          helm repo add stable https://kubernetes-charts.storage.googleapis.com
    ``` 

## How do I Build ?

```bash
# ALL .PY Files in dags/ folder will be loaded as DAGS

make start 
# yup! that's it 
# the command takes few minutes to start, so trust the process & wait.

#Once the Make command is done execute the following commnds in different terminals
kubectl get pods --watch # to monitor the pod health
minikube dashboard  # to get the K8S dashboard

# If this URL doesn't load wait for few mins until the K8S dashboard becomes healthy. (usually takes 6-10 minutes)
minikube service airflow-web -n airflow # to load the Airflow UI page 

# Once you are done with the services you can stop all the services using following command 
make cleanup

```
