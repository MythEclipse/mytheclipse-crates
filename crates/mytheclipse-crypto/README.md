# mytheclipse-crypto

Safe, one-line security helpers that are easy to get wrong when hand-rolled:

- **Argon2id** password hashing & verification (PHC-encoded, RFC 9106-style).
- **AES-256-GCM** authenticated encryption with a fresh random nonce per op.
- **JWT** (HS256) token generation & validation.
- **Key rotation** via `KeyRing` — reads keep working with the previous key
  during a rotation window.

## Features

| Feature | Default | Provides |
| :--- | :---: | :--- |
| `password` | yes | Argon2id hashing and verification (`PasswordHasher`). |
| `encryption` | yes | AES-256-GCM authenticated encryption (`Encryptor`). |
| `tokens` | yes | JWT HS256 signing and validation (`TokenSigner`). Implies `encryption`. |
| `derivation` | no | HKDF-SHA256 key derivation (`HkdfKeyDeriver`). |
| `paseto` | no | **Not implemented — do not use.** `PasetoSigner` emits an unauthenticated base64 envelope, so any token can be forged. |
| `rate-limit` | no | Declared, no implementation yet. |

The three default features are independent and cheap; disable them if you only
need the key-management types.

## Usage

```rust
use mytheclipse_crypto::{PasswordHasher, Encryptor, TokenSigner};

let hasher = PasswordHasher::new();
let hash = hasher.hash("letmein").unwrap();
assert!(hasher.verify(&hash, "letmein"));

let enc = Encryptor::new(&[0u8; 32]);
let (nonce, ct) = enc.encrypt(b"secret");
assert_eq!(enc.decrypt(&nonce, &ct).unwrap(), b"secret");

let signer = TokenSigner::new("my-secret");
let token = signer.sign(&serde_json::json!({"sub":"u1"}), std::time::Duration::from_secs(3600)).unwrap();
assert_eq!(signer.verify(&token).unwrap()["sub"], "u1");
```
