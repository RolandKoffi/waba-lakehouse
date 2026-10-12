import os
import threading
import time
from datetime import date, datetime, timedelta

import numpy as np
import streamlit as st

from data_gen import COUNTRIES, DATASETS, ENTITY_TYPES, Lake, ReferentialStore, generate_and_upload

CACHE = os.environ.get("REF_CACHE", "/data/referentials")
DEFAULT_ROWS = {"bank_transactions": 10_000, "insurance_operations": 5_000,
                "mobile_money_payments": 20_000, "loan_repayments": 5_000}
LABELS = {"bank_transactions": "Transactions bancaires", "insurance_operations": "Opérations d'assurance",
          "mobile_money_payments": "Paiements mobile money", "loan_repayments": "Remboursements de crédit",
          "referentials": "Référentiels"}


@st.cache_resource
def lake():
    return Lake()


@st.cache_resource
def referentials():
    if not os.path.exists(os.path.join(CACHE, "accounts.csv")):
        try:
            lake().download_referentials(CACHE)
        except Exception:
            return None
    return ReferentialStore.load(CACHE)


class ContinuousRunner:
    def __init__(self):
        self.thread = None
        self.stop_event = threading.Event()
        self.log = []

    @property
    def running(self):
        return self.thread is not None and self.thread.is_alive()

    def start(self, refs, datasets, countries, entities, rows, min_s, max_s, anomalies, bad_rate):
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._loop,
            args=(refs, datasets, countries, entities, rows, min_s, max_s, anomalies, bad_rate),
            daemon=True,
        )
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def _loop(self, refs, datasets, countries, entities, rows, min_s, max_s, anomalies, bad_rate):
        rng = np.random.default_rng()
        while not self.stop_event.is_set():
            wait = int(rng.integers(min_s, max_s + 1))
            end = datetime.utcnow().replace(microsecond=0)
            start = end - timedelta(seconds=wait)
            for ds in datasets:
                try:
                    files = generate_and_upload(lake(), refs, ds, countries, rows, start, end, rng,
                                                anomalies, bad_rate, entities)
                    self.log.append(f"{end:%H:%M:%S} {ds}: {len(files)} fichier(s)")
                except Exception as exc:
                    self.log.append(f"{end:%H:%M:%S} ERREUR {ds}: {exc}")
            self.log = self.log[-50:]
            self.stop_event.wait(wait)


@st.cache_resource
def runner():
    return ContinuousRunner()


st.set_page_config(page_title="WABA Data Generator", layout="wide")
st.title("WABA Group — Générateur de données")

data_type = st.selectbox("Type de données", list(LABELS), format_func=LABELS.get)

if data_type == "referentials":
    c1, c2, c3, c4 = st.columns(4)
    n_cust = c1.number_input("Clients", 1_000, 2_000_000, 500_000, step=10_000)
    n_acc = c2.number_input("Comptes", 1_000, 3_000_000, 800_000, step=10_000)
    n_br = c3.number_input("Agences", 16, 2_000, 200)
    n_prod = c4.number_input("Produits", 17, 500, 50)
    if st.button("Générer et envoyer les référentiels", type="primary"):
        with st.spinner("Génération..."):
            refs = ReferentialStore.build(int(n_cust), int(n_acc), int(n_br), int(n_prod))
            refs.save(CACHE)
            keys = lake().upload_referentials(refs)
            referentials.clear()
        st.success("Référentiels envoyés")
        st.write(keys)
    st.stop()

refs = referentials()
if refs is None:
    st.warning("Générez d'abord les référentiels : les transactions doivent référencer des clés existantes.")
    st.stop()

countries = st.multiselect("Pays", COUNTRIES, default=COUNTRIES)
entities = st.multiselect("Lignes métier", ENTITY_TYPES, default=ENTITY_TYPES)
rows = st.number_input("Nombre de lignes", 10, 1_000_000, DEFAULT_ROWS[data_type], step=1000)
today = date.today()
c1, c2 = st.columns(2)
start = c1.date_input("Date de début", today - timedelta(days=90))
end = c2.date_input("Date de fin", today)
anomalies = st.checkbox("Injecter des scénarios suspects (fraude / AML)", value=False)
bad_rate = st.slider("Taux de lignes malformées", 0.0, 0.05, 0.0, 0.005)
mode = st.radio("Mode", ["One-time", "Continue"], horizontal=True)

if not countries or not entities:
    st.stop()

if mode == "One-time":
    if st.button("Générer et envoyer", type="primary"):
        with st.spinner("Génération..."):
            files = generate_and_upload(lake(), refs, data_type, countries, int(rows),
                                        datetime.combine(start, datetime.min.time()),
                                        datetime.combine(end + timedelta(days=1), datetime.min.time()),
                                        np.random.default_rng(), anomalies, bad_rate, entities)
        st.success(f"{len(files)} fichier(s) déposé(s)")
        st.dataframe([{"fichier": f, "lignes": n} for f, n in files])
else:
    datasets = st.multiselect("Flux continus", list(DATASETS), default=[data_type], format_func=LABELS.get)
    batch_rows = st.number_input("Lignes par envoi", 10, 50_000, 200)
    interval = st.slider("Intervalle (secondes)", 10, 60, (10, 60))
    r = runner()
    c1, c2 = st.columns(2)
    if c1.button("Démarrer", disabled=r.running, type="primary"):
        r.start(refs, datasets, countries, entities, int(batch_rows), interval[0], interval[1], anomalies, bad_rate)
        time.sleep(1)
        st.rerun()
    if c2.button("Arrêter", disabled=not r.running):
        r.stop()
        st.rerun()
    st.write("En cours" if r.running else "Arrêté")
    st.code("\n".join(reversed(r.log)) or "-")
