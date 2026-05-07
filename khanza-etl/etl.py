import subprocess
import os
import datetime

SCRIPT_DIR = r"E:\Downloads\visuadata\khanza-etl"
LOG_DIR = os.path.join(SCRIPT_DIR, "log")

os.makedirs(LOG_DIR, exist_ok=True)

DIM_SCRIPTS = [
    "dim_tanggal.py",
    "dim_pasien.py",
    "dim_dokter.py",
    "dim_pegawai.py",
    "dim_poli.py",
    "dim_bangsal.py",
    "dim_kamar.py",
    "dim_barang.py",
    "dim_supliermedis.py",
    "dim_supliernonmedis.py",
    "dim_jabatan.py",
    "dim_departemen.py",
    "dim_kategoriobat.py",
    "dim_alkes.py",
    "dim_labor.py",
    "dim_paketoperasi.py",
    "dim_carabayar.py",
    "dim_sep.py",
    "dim_sipstr.py",
    "dim_icd9.py",
    "dim_icd10.py",
    "dim_kontrak.py",
    "dim_satuannonmedis.py",
]

FACT_SCRIPTS = [
    "fact_antrean_per_tanggal.py",
    "fact_registrasi.py",
    "fact_rawatinap.py",
    "fact_operasi.py",
    "fact_prosedur_pasien.py",
    "fact_diagnosa_pasien.py",
    "fact_lab.py",
    "fact_resep.py",
    "fact_stokobat.py",
    "fact_pemakaianlogistik.py",
    "fact_belanjalogistik.py",
    "fact_billing.py"
]

def run_script(script_name):
    script_path = os.path.join(SCRIPT_DIR, script_name)
    log_file = os.path.join(LOG_DIR, f"{script_name.replace('.py', '')}.log")

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"\n=== {datetime.datetime.now()} : Mulai {script_name} ===\n")
        try:
            result = subprocess.run(
                ["python", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False
            )
            f.write(result.stdout)
            if result.stderr:
                f.write("\n[ERROR]\n")
                f.write(result.stderr)
            f.write(f"\n=== {datetime.datetime.now()} : Selesai {script_name} ===\n")
        except Exception as e:
            f.write(f"\n[EXCEPTION] {e}\n")

def main():
    print("=== Mulai ETL Process ===")

    print("Menjalankan DIMENSIONS...")
    for script in DIM_SCRIPTS:
        print(f" -> {script}")
        run_script(script)

    print("Menjalankan FACTS...")
    for script in FACT_SCRIPTS:
        print(f" -> {script}")
        run_script(script)

    print("=== ETL Selesai, cek folder log ===")

if __name__ == "__main__":
    main()

