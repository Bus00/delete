
import subprocess

def run_step(step_name, command):
    print(f"\n Running {step_name}...")

    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"❌ {step_name} failed.")
        exit(1)

    print(f"✅ {step_name} completed.")


if __name__ == "__main__":

    run_step("Log Generation", "python src/1generate_logs.py")
    run_step("Parsing", "python src/2parser.py")
    run_step("Validation", "python src/3validator.py")
    run_step("Risk Engine", "python src/4risk_engine.py")
    run_step("Pattern Analysis", "python src/5pattern_analysis.py") 
    run_step("Database Save", "python src/6database.py")
    run_step("Reporting", "python src/7reporting.py")

    print("\n Pipeline successfully executed.")
