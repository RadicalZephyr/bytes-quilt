# BytesQuilt

BytesQuilt is a Rust library that provides an efficient and flexible
way to handle binary data buffers. It allows you to write bytes in any
order and efficiently reassemble the entire binary buffer without
copying any data.

## Features

- **Efficient Byte Management:** BytesQuilt internally uses the
  [Bytes](https://docs.rs/bytes) crate to handle the underlying
  buffer, ensuring efficient memory management and optimal
  performance.

- **Partial Write Tracking:** BytesQuilt allows you to track which
  portions of the buffer have been written to. This provides
  visibility into the progress of writing to the buffer.

- **Unread Portion Querying:** BytesQuilt enables you to query the
  portions of the buffer that have not yet been received. This
  capability allows for targeted data retrieval and helps in
  implementing efficient data processing workflows.

- **Fast Insertion with Binary Search:** BytesQuilt supports fast
  insertion to any part of the buffer using binary search to
  efficiently locate the correct region.

- **Seamless Reassembly:** BytesQuilt maintains meticulous bookkeeping
  information, enabling the reassembly of the entire binary buffer
  without any copying. This approach minimizes memory overhead and
  preserves performance.

## Getting Started

To use BytesQuilt in your Rust project, simply add the following line
to your `Cargo.toml` file:

```toml
[dependencies]
bytesquilt = "<version>"
```

Make sure to replace `<version>` with the appropriate version you want
to use, following the semantic versioning guidelines.

## Usage

Here's a simple example that demonstrates how to use BytesQuilt:

```rust
use bytesquilt::BytesQuilt;

fn main() {
    // Create a new BytesQuilt instance
    let mut quilt = BytesQuilt::new();

    // Write bytes to the quilt
    quilt.put_u8_at(32, 0x04);
    quilt.put_bytes_at(0, 0x01, 2);
    quilt.put_at(16, &[0x02, 0x03]);
    quilt.put_u32(40, 0x05);

    // Reassemble the buffer
    let buffer = quilt.reassemble();

    // Use the buffer as needed
    println!("Buffer: {:?}", buffer);
    // Buffer: [0x01, 0x01, 0x02, 0x03, 0x04, 0x05]
}
```

For more details and advanced usage examples, please refer to the
[documentation](https://docs.rs/bytes-quilt).

## Contributing

Contributions to BytesQuilt are welcome! If you encounter any issues,
have suggestions, or would like to contribute new features or
improvements, please open an issue or submit a pull request.

Please follow the [contribution guidelines](CONTRIBUTING.md) when
submitting pull requests.

## License

&copy; Zefira Shannon | 2022-2023

Licensed under the [Apache License](LICENSE), Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.  See the License for the specific language governing
permissions and limitations under the License.
