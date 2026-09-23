pub mod wire {
    #[repr(C)]
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct VersionedTelemetryFrame {
        pub sequence_id: u64,
        pub timestamp_ns: u64,
        pub device_id_hash: u64,
        pub signal_metrics: [f32; 6],
        pub anomaly_score: f32,
        pub nonce: u32,
        pub checksum: u32,
        pub magic_header: u16,
        pub version: u8,
        pub flags: u8,
    }

    pub const FRAME_SIZE: usize = 64;
    pub const HEADER_SIZE: usize = 56;

    pub const MAGIC: u16 = 0x4153;
    pub const VERSION: u8 = 0x01;

    const _: () = assert!(
        core::mem::size_of::<VersionedTelemetryFrame>() == FRAME_SIZE
    );

    const _: () = assert!(
        core::mem::align_of::<VersionedTelemetryFrame>() == 8
    );

    impl VersionedTelemetryFrame {
        #[inline]
        pub fn decode_from_wire(
            bytes: &[u8],
        ) -> Result<Self, &'static str> {
            if bytes.len() != FRAME_SIZE {
                return Err("INVALID_FRAME_LENGTH");
            }

            let magic = u16::from_le_bytes([bytes[60], bytes[61]]);
            if magic != MAGIC {
                return Err("INVALID_MAGIC_HEADER");
            }

            if bytes[62] != VERSION {
                return Err("UNSUPPORTED_PROTOCOL_VERSION");
            }

            let checksum =
                u32::from_le_bytes([bytes[56], bytes[57], bytes[58], bytes[59]]);

            let computed = crc32c::crc32c(&bytes[..HEADER_SIZE]);
            if computed != checksum {
                return Err("CHECKSUM_MISMATCH");
            }

            let sequence_id =
                u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            let timestamp_ns =
                u64::from_le_bytes(bytes[8..16].try_into().unwrap());
            let device_id_hash =
                u64::from_le_bytes(bytes[16..24].try_into().unwrap());

            let mut signal_metrics = [0.0_f32; 6];
            let mut i = 0;
            while i < 6 {
                let offset = 24 + i * 4;
                let bits = u32::from_le_bytes(
                    bytes[offset..offset + 4].try_into().unwrap()
                );
                let value = f32::from_bits(bits);
                if !value.is_finite() {
                    return Err("INVALID_METRIC_VALUE");
                }
                signal_metrics[i] = value;
                i += 1;
            }

            let anomaly_score = f32::from_bits(
                u32::from_le_bytes(bytes[48..52].try_into().unwrap())
            );
            if !anomaly_score.is_finite()
                || !(0.0..=1.0).contains(&anomaly_score)
            {
                return Err("INVALID_ANOMALY_SCORE");
            }

            let nonce =
                u32::from_le_bytes(bytes[52..56].try_into().unwrap());

            Ok(Self {
                sequence_id,
                timestamp_ns,
                device_id_hash,
                signal_metrics,
                anomaly_score,
                nonce,
                checksum,
                magic_header: magic,
                version: bytes[62],
                flags: bytes[63],
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::VersionedTelemetryFrame;

        #[test]
        fn test_canonical_golden_vector_decoding() {
            let hex_vector = concat!(
                "6500000000000000",
                "15cd853dfe9c9717",
                "efcdab1032547698",
                "cdcccc3dcdcc4cbe",
                "9a99993ecdccccbe",
                "0000003f9a9919bf",
                "0000603fd4c3b2a1",
                "7f5edcb1",
                "53410101",
            );
            assert_eq!(hex_vector.len(), 128);
            let bytes = hex::decode(hex_vector).expect("decode hex");
            assert_eq!(bytes.len(), 64);

            let decoded = VersionedTelemetryFrame::decode_from_wire(&bytes)
                .expect("Canonical golden vector failed validation");

            assert_eq!(decoded.sequence_id, 101);
            assert_eq!(decoded.timestamp_ns, 1_700_000_000_123_456_789);
            assert_eq!(decoded.device_id_hash, 0x9876543210ABCDEF);
            assert_eq!(
                decoded.signal_metrics,
                [0.1f32, -0.2f32, 0.3f32, -0.4f32, 0.5f32, -0.6f32]
            );
            assert_eq!(decoded.anomaly_score, 0.875f32);
            assert_eq!(decoded.nonce, 0xA1B2C3D4);
            assert_eq!(decoded.checksum, 0xB1DC5E7F);
            assert_eq!(decoded.magic_header, 0x4153);
            assert_eq!(decoded.version, 0x01);
            assert_eq!(decoded.flags, 0x01);
        }

        #[test]
        fn test_golden_vector_is_exactly_64_bytes() {
            let hex_vector = concat!(
                "6500000000000000",
                "15cd853dfe9c9717",
                "efcdab1032547698",
                "cdcccc3dcdcc4cbe",
                "9a99993ecdccccbe",
                "0000003f9a9919bf",
                "0000603fd4c3b2a1",
                "7f5edcb1",
                "53410101",
            );
            let bytes = hex::decode(hex_vector).expect("valid hex");
            assert_eq!(bytes.len(), 64);
            assert_eq!(&bytes[56..60], &[0x7f, 0x5e, 0xdc, 0xb1]);
            assert_eq!(&bytes[60..62], &[0x53, 0x41]);
            assert_eq!(bytes[62], 0x01);
            assert_eq!(bytes[63], 0x01);
        }

        #[test]
        fn test_crc32c_matches_wire_checksum() {
            let hex_vector = concat!(
                "6500000000000000",
                "15cd853dfe9c9717",
                "efcdab1032547698",
                "cdcccc3dcdcc4cbe",
                "9a99993ecdccccbe",
                "0000003f9a9919bf",
                "0000603fd4c3b2a1",
                "7f5edcb1",
                "53410101",
            );
            let bytes = hex::decode(hex_vector).expect("valid hex");
            let computed = crc32c::crc32c(&bytes[0..56]);
            assert_eq!(computed, 0xB1DC5E7F);
            let wire = u32::from_le_bytes(bytes[56..60].try_into().unwrap());
            assert_eq!(wire, computed);
        }
    }
}
