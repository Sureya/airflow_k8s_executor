.PHONY: build-image build-helm
.ONESHELL:


export BASE_PATH ?= $(shell pwd)
export BASH ?= $(BASE_PATH)/bash_scripts
export AIRFLOW_DOCKER_PATH ?= $(BASE_PATH)/docker_airflow_image/puckel/docker-airflow/
export AIRFLOW_HELM_PATH ?= $(BASE_PATH)/helm_charts/official/charts/stable/airflow/
export AIRFLOW_DAGS_PATH ?= $(BASE_PATH)/dags/
export AIRFLOW_HELM_CHART ?= $(BASE_PATH)/helm_charts/official/charts/airflow.yaml


build:
	minikube start --vm-driver=virtualbox --memory=6096 --disk-size=20000mb --kubernetes-version v1.15.0
	@eval $$(minikube docker-env) ;\
	kubectl config set-context minikube --cluster=minikube --namespace=airflow; \
	kubectl delete namespace airflow || true ; \
	kubectl create namespace airflow ; \
	docker build -t airflow-docker-local:1  $(AIRFLOW_DOCKER_PATH) --no-cache
	docker run -d -p 5000:5000 --restart=always --name registry registry:2

deploy:
	@eval $$(minikube docker-env) ; \
	helm delete airflow || true ; \
	helm dependency build $(AIRFLOW_HELM_PATH) ; \
	helm install airflow  -f $(AIRFLOW_HELM_CHART) $(AIRFLOW_HELM_PATH)
	bash $(BASH)/load_dags.sh $(AIRFLOW_DAGS_PATH) 30

run:
	make build
	make deploy

restart:
	make cleanup
	make run

redeploy:
	helm delete airflow || true
	make deploy

cleanup:
	bash $(BASH)/clean_registry.sh
	helm delete airflow || true
	minikube delete

dags:
	./load_dags.sh $(AIRFLOW_DAGS_PATH) 1

