"""
Gerador de Dados de Amostra — VidaPlus Saúde

Gera datasets realistas para desenvolvimento e testes:
- Pacientes (JSON)
- Consultas médicas (CSV)
- Exames laboratoriais (JSON aninhado)
- Sinais vitais IoT (JSON streaming)

Uso:
    pip install faker
    python data/sample/generate_data.py
"""

import json
import csv
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    print("Instale o faker: pip install faker")
    raise

fake = Faker("pt_BR")
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path(__file__).parent
PACIENTES_FILE = OUTPUT_DIR / "pacientes.json"
CONSULTAS_FILE = OUTPUT_DIR / "consultas.csv"
EXAMES_FILE = OUTPUT_DIR / "exames_laboratorio.json"
SINAIS_FILE = OUTPUT_DIR / "sinais_vitais.json"

# Configurações
NUM_PACIENTES = 500
NUM_CONSULTAS = 2000
NUM_EXAMES = 800
NUM_SINAIS = 5000

UNIDADES = [f"HOSP-{str(i).zfill(3)}" for i in range(1, 21)]
CLINICAS = [f"CLIN-{str(i).zfill(3)}" for i in range(1, 61)]

ESPECIALIDADES = [
    "Cardiologia", "Ortopedia", "Neurologia", "Dermatologia",
    "Ginecologia", "Pediatria", "Oftalmologia", "Urologia",
    "Endocrinologia", "Gastroenterologia", "Pneumologia",
    "Psiquiatria", "Oncologia", "Nefrologia", "Reumatologia",
]

CONVENIOS = [
    "SUS", "Unimed", "Bradesco Saúde", "Amil", "SulAmérica",
    "NotreDame Intermédica", "Hapvida", "Particular",
]

STATUS_CONSULTA = ["REALIZADA", "CANCELADA", "NO_SHOW", "AGENDADA"]

CID_CODES = [
    "I10", "E11", "J06", "M54", "K29", "N39", "J45", "F32",
    "E78", "I25", "J18", "K80", "M75", "G43", "L50",
]

ANALITOS_HEMOGRAMA = [
    ("Hemoglobina", "g/dL", 12.0, 17.5),
    ("Hematócrito", "%", 36.0, 50.0),
    ("Leucócitos", "mil/mm³", 4.0, 11.0),
    ("Plaquetas", "mil/mm³", 150.0, 400.0),
    ("Hemácias", "milhões/mm³", 4.0, 5.8),
    ("VCM", "fL", 80.0, 100.0),
    ("HCM", "pg", 27.0, 33.0),
]

ANALITOS_BIOQUIMICA = [
    ("Glicose", "mg/dL", 70.0, 99.0),
    ("Colesterol Total", "mg/dL", 0.0, 200.0),
    ("HDL", "mg/dL", 40.0, 60.0),
    ("LDL", "mg/dL", 0.0, 130.0),
    ("Triglicerídeos", "mg/dL", 0.0, 150.0),
    ("Creatinina", "mg/dL", 0.6, 1.2),
    ("Ureia", "mg/dL", 15.0, 40.0),
    ("TGO/AST", "U/L", 10.0, 40.0),
    ("TGP/ALT", "U/L", 7.0, 56.0),
]


def generate_pacientes():
    """Gera dados de pacientes em JSON."""
    pacientes = []
    for i in range(1, NUM_PACIENTES + 1):
        paciente = {
            "paciente_id": f"PAC-{str(i).zfill(6)}",
            "nome": fake.name(),
            "cpf": fake.cpf(),
            "data_nascimento": fake.date_of_birth(minimum_age=1, maximum_age=95).isoformat(),
            "sexo": random.choice(["M", "F"]),
            "telefone": fake.phone_number(),
            "email": fake.email(),
            "endereco": {
                "logradouro": fake.street_name(),
                "numero": str(fake.random_int(1, 9999)),
                "bairro": fake.bairro(),
                "cidade": fake.city(),
                "estado": fake.estado_sigla(),
                "cep": fake.postcode(),
            },
            "convenio": random.choice(CONVENIOS),
            "tipo_sanguineo": random.choice(
                ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
            ),
            "alergias": random.sample(
                ["Penicilina", "Dipirona", "AAS", "Látex", "Iodo", "Nenhuma"],
                k=random.randint(1, 3),
            ),
            "condicoes_cronicas": random.sample(
                [
                    "Hipertensão", "Diabetes Tipo 2", "Asma", "Artrite",
                    "Depressão", "Hipotireoidismo", "Nenhuma",
                ],
                k=random.randint(0, 2),
            ),
            "unidade_principal": random.choice(UNIDADES),
            "created_at": fake.date_time_between(
                start_date="-5y", end_date="now"
            ).isoformat(),
        }
        pacientes.append(paciente)

    with open(PACIENTES_FILE, "w", encoding="utf-8") as f:
        json.dump(pacientes, f, ensure_ascii=False, indent=2)

    print(f"  ✓ {len(pacientes)} pacientes → {PACIENTES_FILE}")
    return [p["paciente_id"] for p in pacientes]


def generate_consultas(paciente_ids):
    """Gera dados de consultas médicas em CSV."""
    with open(CONSULTAS_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "consulta_id", "paciente_id", "medico_id", "unidade_id",
            "especialidade", "data_agendamento", "data_consulta",
            "hora_consulta", "status", "tipo_consulta", "convenio",
            "valor", "cid_principal", "tempo_espera_min", "no_show",
            "created_at",
        ])

        for i in range(1, NUM_CONSULTAS + 1):
            data_consulta = fake.date_between(start_date="-2y", end_date="today")
            data_agendamento = data_consulta - timedelta(days=random.randint(1, 30))
            status = random.choices(
                STATUS_CONSULTA, weights=[60, 15, 10, 15], k=1
            )[0]

            writer.writerow([
                f"CON-{str(i).zfill(8)}",
                random.choice(paciente_ids),
                f"MED-{str(random.randint(1, 200)).zfill(4)}",
                random.choice(UNIDADES + CLINICAS),
                random.choice(ESPECIALIDADES),
                data_agendamento.isoformat(),
                data_consulta.isoformat(),
                f"{random.randint(7, 19):02d}:{random.choice(['00', '15', '30', '45'])}",
                status,
                random.choice(["PRIMEIRA_VEZ", "RETORNO", "URGENCIA"]),
                random.choice(CONVENIOS),
                f"R$ {random.uniform(80, 800):.2f}".replace(".", ","),
                random.choice(CID_CODES) if status == "REALIZADA" else "",
                random.randint(0, 120) if status == "REALIZADA" else "",
                "SIM" if status == "NO_SHOW" else "NAO",
                datetime.combine(
                    data_agendamento, datetime.min.time()
                ).isoformat(),
            ])

    print(f"  ✓ {NUM_CONSULTAS} consultas → {CONSULTAS_FILE}")


def generate_exames(paciente_ids):
    """Gera dados de exames laboratoriais em JSON aninhado."""
    exames = []

    for i in range(1, NUM_EXAMES + 1):
        data_coleta = fake.date_between(start_date="-2y", end_date="today")

        paineis = []
        selected_paineis = random.sample(
            [("Hemograma Completo", "40304361", ANALITOS_HEMOGRAMA),
             ("Bioquímica", "40301630", ANALITOS_BIOQUIMICA)],
            k=random.randint(1, 2),
        )

        for painel_nome, codigo_tuss, analitos in selected_paineis:
            resultados = []
            for analito, unidade, ref_min, ref_max in analitos:
                mean = (ref_min + ref_max) / 2
                std = (ref_max - ref_min) / 4
                valor = round(random.gauss(mean, std), 2)
                valor = max(0, valor)

                flags = []
                if valor < ref_min:
                    flags.append("BAIXO")
                elif valor > ref_max:
                    flags.append("ALTO")
                else:
                    flags.append("NORMAL")

                resultados.append({
                    "analito": analito,
                    "valor": valor,
                    "unidade": unidade,
                    "referencia": {
                        "min": ref_min,
                        "max": ref_max,
                        "unidade": unidade,
                    },
                    "flags": flags,
                })

            paineis.append({
                "nome": painel_nome,
                "codigo_tuss": codigo_tuss,
                "resultados": resultados,
            })

        exame = {
            "paciente_id": random.choice(paciente_ids),
            "unidade_id": random.choice(UNIDADES),
            "ordem_exame": {
                "ordem_id": f"ORD-{str(i).zfill(8)}",
                "data_coleta": data_coleta.isoformat(),
                "data_resultado": (
                    data_coleta + timedelta(days=random.randint(1, 5))
                ).isoformat(),
                "medico_solicitante": {
                    "crm": f"CRM-{fake.estado_sigla()}-{random.randint(10000, 99999)}",
                    "nome": f"Dr(a). {fake.name()}",
                    "especialidade": random.choice(ESPECIALIDADES),
                },
                "paineis": paineis,
                "status": random.choice(["FINALIZADO", "PENDENTE", "CANCELADO"]),
                "urgente": random.choice(["SIM", "NAO"]),
            },
            "created_at": datetime.combine(
                data_coleta, datetime.min.time()
            ).isoformat(),
        }
        exames.append(exame)

    with open(EXAMES_FILE, "w", encoding="utf-8") as f:
        for exame in exames:
            f.write(json.dumps(exame, ensure_ascii=False) + "\n")

    print(f"  ✓ {len(exames)} exames (JSON aninhado) → {EXAMES_FILE}")


def generate_sinais_vitais(paciente_ids):
    """Gera dados de sinais vitais IoT em JSON (1 registro por linha)."""
    pacientes_uti = random.sample(paciente_ids, min(50, len(paciente_ids)))
    devices = {
        pid: f"DEV-{str(i).zfill(4)}"
        for i, pid in enumerate(pacientes_uti, 1)
    }
    leitos = {
        pid: f"UTI-{random.choice(UNIDADES)}-{random.randint(1, 30):02d}"
        for pid in pacientes_uti
    }

    with open(SINAIS_FILE, "w", encoding="utf-8") as f:
        base_time = datetime.now() - timedelta(hours=24)

        for i in range(NUM_SINAIS):
            pid = random.choice(pacientes_uti)
            timestamp = base_time + timedelta(seconds=i * 17)

            is_anomaly = random.random() < 0.05

            if is_anomaly:
                fc = random.choice([random.randint(30, 49), random.randint(121, 180)])
                pa_sis = random.choice([random.randint(60, 79), random.randint(181, 220)])
                spo2 = round(random.uniform(85.0, 91.9), 1)
            else:
                fc = random.randint(60, 100)
                pa_sis = random.randint(100, 140)
                spo2 = round(random.uniform(95.0, 100.0), 1)

            sinal = {
                "device_id": devices[pid],
                "paciente_id": pid,
                "leito": leitos[pid],
                "unidade_id": leitos[pid].split("-")[1],
                "timestamp": timestamp.isoformat(),
                "frequencia_cardiaca": fc,
                "pressao_sistolica": pa_sis,
                "pressao_diastolica": random.randint(60, 90),
                "saturacao_o2": spo2,
                "temperatura": round(random.gauss(36.5, 0.5), 1),
                "frequencia_respiratoria": random.randint(12, 25),
            }

            # Inserir nulos aleatoriamente (simula falha de sensor)
            if random.random() < 0.02:
                campo = random.choice([
                    "frequencia_cardiaca", "pressao_sistolica", "saturacao_o2"
                ])
                sinal[campo] = None

            f.write(json.dumps(sinal, ensure_ascii=False) + "\n")

    print(f"  ✓ {NUM_SINAIS} leituras IoT → {SINAIS_FILE}")


def main():
    print("Gerando dados de amostra VidaPlus Saúde...")
    print(f"Diretório: {OUTPUT_DIR}\n")

    paciente_ids = generate_pacientes()
    generate_consultas(paciente_ids)
    generate_exames(paciente_ids)
    generate_sinais_vitais(paciente_ids)

    print("\nDados gerados com sucesso!")
    print("\nPara fazer upload para o S3:")
    print("  aws s3 cp data/sample/pacientes.json s3://vidaplus-raw-dev/pacientes/")
    print("  aws s3 cp data/sample/consultas.csv s3://vidaplus-raw-dev/consultas/")
    print("  aws s3 cp data/sample/exames_laboratorio.json s3://vidaplus-raw-dev/exames_laboratorio/")
    print("  aws s3 cp data/sample/sinais_vitais.json s3://vidaplus-raw-dev/sinais_vitais/")


if __name__ == "__main__":
    main()
