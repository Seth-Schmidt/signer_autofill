
async def write_transaction(w3, chain_id, from_address, private_key, to_address, value, nonce):
    """ Writes a transaction to the blockchain

    Args:
            w3 (web3.Web3): Web3 object
            address (str): The address of the account
            private_key (str): The private key of the account

    Returns:
            str: The transaction hash
    """
    # create the transaction dict
    transaction = {
        'from': from_address,
        'to': to_address,
        'gas': 21000,
        'maxFeePerGas': w3.to_wei('0.01', 'gwei'),
        'maxPriorityFeePerGas': int(0.1 * w3.to_wei('0.01', 'gwei')),
        'nonce': nonce,
        'chainId': chain_id,
        'value': value,
    }

    # Sign the transaction
    # ref: https://web3py.readthedocs.io/en/v5/web3.eth.html#web3.eth.Eth.send_raw_transaction
    signed_transaction = w3.eth.account.sign_transaction(
        transaction, private_key
    )
    # Send the transaction
    tx_hash = await w3.eth.send_raw_transaction(signed_transaction.rawTransaction)
    # Wait for confirmation
    return tx_hash.hex()
