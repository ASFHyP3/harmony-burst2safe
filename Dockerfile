FROM mambaorg/micromamba:latest

WORKDIR /home/mambauser

COPY --chown=$MAMBA_USER:$MAMBA_USER . /harmony-burst2safe/

RUN micromamba install -y -n base -f /harmony-burst2safe/environment.yml && \
    micromamba install -y -n base git && \
    micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1
RUN python -m pip install -e /harmony-burst2safe/

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python", "-m", "harmony_burst2safe.harmony_service"]
