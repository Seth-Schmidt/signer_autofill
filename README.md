Setup:

- $ `poetry install`
- Copy `config/settings.example.json` to `config/settings.json`
- Populate the following lines with the necessary information in `config/settings.json`:
    - "source_address": "source-account-address"
    - "source_chain_id": "source-chain-id"
    - "source_private_key": "source-account-private-key"
- Add the signers you wish to supply with ETH in the `snapshot_submissions.signers` section of `config/settings.json`
- Change `min_signer_value` (optional)

Run:

`poetry run python3 -m supply_signers`