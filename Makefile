# Mutation testing of airflow_provider_rivet/ (README, "Mutation testing"), in a Linux container (dev/mutants.sh).
# RIVET_SRC: a rivet checkout at RIVET_CONTRACT_REF, as for `pytest` (default ../rivet).

.PHONY: mutants mutants-results mutants-clean

mutants:  ## run every mutant (resumes: finished mutants keep their result in mutants/), then list the survivors
	dev/mutants.sh
	dev/mutants.sh mutmut results

mutants-results:  ## the survivors, timeouts and other non-killed mutants of the last run
	dev/mutants.sh mutmut results

mutants-clean:  ## forget every result: the next `make mutants` starts over
	rm -rf mutants
