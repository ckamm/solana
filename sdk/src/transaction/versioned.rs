//! Defines a transaction which supports multiple versions of messages.

#![cfg(feature = "full")]

use {
    crate::{
        hash::Hash,
        instruction::Instruction,
        message::{self, v0::AddressLookupTable, VersionedMessage},
        precompiles::verify_if_precompile,
        pubkey::Pubkey,
        sanitize::{Sanitize, SanitizeError},
        short_vec,
        signature::{Signature, SignerError},
        signers::Signers,
        transaction::{Result, Transaction, TransactionError},
    },
    serde::Serialize,
    std::result,
};

// NOTE: Serialization-related changes must be paired with the direct read at sigverify.
/// An atomic transaction
#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize, AbiExample)]
pub struct VersionedTransaction {
    /// List of signatures
    #[serde(with = "short_vec")]
    pub signatures: Vec<Signature>,
    /// Message to sign.
    pub message: VersionedMessage,
}

impl Sanitize for VersionedTransaction {
    fn sanitize(&self) -> std::result::Result<(), SanitizeError> {
        self.message.sanitize()?;

        // Once the "verify_tx_signatures_len" feature is enabled, this may be
        // updated to an equality check.
        if usize::from(self.message.header().num_required_signatures) > self.signatures.len() {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        // Signatures are verified before message keys are loaded so all signers
        // must correspond to static account keys.
        if self.signatures.len() > self.message.static_account_keys_len() {
            return Err(SanitizeError::IndexOutOfBounds);
        }

        Ok(())
    }
}

impl From<Transaction> for VersionedTransaction {
    fn from(transaction: Transaction) -> Self {
        Self {
            signatures: transaction.signatures,
            message: VersionedMessage::Legacy(transaction.message),
        }
    }
}

impl VersionedTransaction {
    pub fn new_unsigned(message: VersionedMessage) -> Self {
        Self {
            signatures: vec![
                Signature::default();
                message.header().num_required_signatures as usize
            ],
            message,
        }
    }

    pub fn new_with_payer(
        instructions: &[Instruction],
        payer: Option<&Pubkey>,
        address_lookup_tables: Option<&[AddressLookupTable]>,
    ) -> Self {
        let message = VersionedMessage::V0(message::v0::Message::new_with_payer(
            instructions,
            payer,
            address_lookup_tables,
        ));
        Self::new_unsigned(message)
    }

    /// Returns a legacy transaction if the transaction message is legacy.
    pub fn into_legacy_transaction(self) -> Option<Transaction> {
        match self.message {
            VersionedMessage::Legacy(message) => Some(Transaction {
                signatures: self.signatures,
                message,
            }),
            _ => None,
        }
    }

    /// Verify the transaction and hash its message
    pub fn verify_and_hash_message(&self) -> Result<Hash> {
        let message_bytes = self.message.serialize();
        if !self
            ._verify_with_results(&message_bytes)
            .iter()
            .all(|verify_result| *verify_result)
        {
            Err(TransactionError::SignatureFailure)
        } else {
            Ok(VersionedMessage::hash_raw_message(&message_bytes))
        }
    }

    /// Verify the transaction and return a list of verification results
    pub fn verify_with_results(&self) -> Vec<bool> {
        let message_bytes = self.message.serialize();
        self._verify_with_results(&message_bytes)
    }

    fn _verify_with_results(&self, message_bytes: &[u8]) -> Vec<bool> {
        self.signatures
            .iter()
            .zip(self.message.static_account_keys_iter())
            .map(|(signature, pubkey)| signature.verify(pubkey.as_ref(), message_bytes))
            .collect()
    }

    /// Check keys and keypair lengths, then sign this transaction.
    ///
    /// # Panics
    ///
    /// Panics when signing fails, use [`Transaction::try_sign`] to handle the error.
    pub fn sign<T: Signers>(&mut self, keypairs: &T, recent_blockhash: Hash) {
        if let Err(e) = self.try_sign(keypairs, recent_blockhash) {
            panic!("Transaction::sign failed with error {:?}", e);
        }
    }

    /// Check keys and keypair lengths, then sign this transaction, returning any signing errors
    /// encountered
    pub fn try_sign<T: Signers>(
        &mut self,
        keypairs: &T,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        self.try_partial_sign(keypairs, recent_blockhash)?;

        if !self.is_signed() {
            Err(SignerError::NotEnoughSigners)
        } else {
            Ok(())
        }
    }

    ///  Sign using some subset of required keys, returning any signing errors encountered. If
    ///  recent_blockhash is not the same as currently in the transaction, clear any prior
    ///  signatures and update recent_blockhash
    pub fn try_partial_sign<T: Signers>(
        &mut self,
        keypairs: &T,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        let positions = self.get_signing_keypair_positions(&keypairs.pubkeys())?;
        if positions.iter().any(|pos| pos.is_none()) {
            return Err(SignerError::KeypairPubkeyMismatch);
        }
        let positions: Vec<usize> = positions.iter().map(|pos| pos.unwrap()).collect();
        self.try_partial_sign_unchecked(keypairs, positions, recent_blockhash)
    }

    /// Sign the transaction, returning any signing errors encountered, and place the
    /// signatures in their associated positions in `signatures` without checking that the
    /// positions are correct.
    pub fn try_partial_sign_unchecked<T: Signers>(
        &mut self,
        keypairs: &T,
        positions: Vec<usize>,
        recent_blockhash: Hash,
    ) -> result::Result<(), SignerError> {
        // if you change the blockhash, you're re-signing...
        if recent_blockhash != *self.message.recent_blockhash() {
            self.message.set_recent_blockhash(recent_blockhash);
            self.signatures
                .iter_mut()
                .for_each(|signature| *signature = Signature::default());
        }

        let message_bytes = self.message.serialize();
        let signatures = keypairs.try_sign_message(&message_bytes)?;
        for i in 0..positions.len() {
            self.signatures[positions[i]] = signatures[i];
        }
        Ok(())
    }

    /// Get the positions of the pubkeys in `account_keys` associated with signing keypairs
    pub fn get_signing_keypair_positions(&self, pubkeys: &[Pubkey]) -> Result<Vec<Option<usize>>> {
        let header = self.message.header();
        let static_account_keys = self.message.static_account_keys();
        if static_account_keys.len() < header.num_required_signatures as usize {
            return Err(TransactionError::InvalidAccountIndex);
        }
        let signed_keys = &static_account_keys[0..header.num_required_signatures as usize];

        Ok(pubkeys
            .iter()
            .map(|pubkey| signed_keys.iter().position(|x| x == pubkey))
            .collect())
    }

    pub fn is_signed(&self) -> bool {
        self.signatures
            .iter()
            .all(|signature| *signature != Signature::default())
    }
}
