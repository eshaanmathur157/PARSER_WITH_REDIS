import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import sys

class SolanaFlightServer(flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(SolanaFlightServer, self).__init__(location, **kwargs)
        
        # --- CONFIGURATION: Your Watchlist ---
        self.BASE_VAULTS = {"vault_addr_1", "vault_addr_2"}
        self.QUOTE_VAULTS = {"quote_addr_1", "quote_addr_2"}
        self.BASE_MINTS = {"mint_addr_ABC", "mint_addr_DEF"}
        self.QUOTE_MINTS = {"mint_addr_XYZ", "mint_addr_123"}
        
        print(f"Server running at: {location}")
    
    def do_put(self, context, descriptor, reader, writer):
        print(f"\n[NEW STREAM] Path: {descriptor.path}")
        sys.stdout.flush()  # Force print immediately
        
        try:
            print("  → Starting to read chunks...")
            sys.stdout.flush()
            
            chunk_count = 0
            while True:
                try:
                    print(f"  → Attempting to read chunk {chunk_count}...")
                    sys.stdout.flush()
                    
                    batch, metadata = reader.read_chunk()
                    
                    print(f"  → Read chunk {chunk_count}: batch={'None' if batch is None else f'{batch.num_rows} rows'}, metadata={'None' if metadata is None else 'present'}")
                    sys.stdout.flush()
                    
                    if batch is None:
                        print("  → Batch is None, breaking")
                        sys.stdout.flush()
                        break
                    
                    chunk_count += 1
                    
                    # --- STEP 1: Extract Timestamp from metadata buffer ---
                    ts_val = 0
                    try:
                        if metadata:
                            print(f"  → Processing metadata...")
                            sys.stdout.flush()
                            
                            metadata_str = metadata.to_pybytes().decode('utf-8')
                            print(f"  → Metadata string: {metadata_str}")
                            sys.stdout.flush()
                            
                            if metadata_str.startswith("timestamp:"):
                                ts_val = int(metadata_str.split(":", 1)[1])
                                print(f"  ✓ Parsed timestamp: {ts_val}")
                                sys.stdout.flush()
                        else:
                            print("  → No metadata in this chunk")
                            sys.stdout.flush()
                    except Exception as e:
                        print(f"  ✗ Metadata parsing error: {e}")
                        sys.stdout.flush()
                        import traceback
                        traceback.print_exc()
                    
                    # --- STEP 2: Convert to Pandas ---
                    print(f"  → Converting batch to pandas...")
                    sys.stdout.flush()
                    
                    df = batch.to_pandas()
                    df['timestamp'] = ts_val
                    
                    print(f"  → DataFrame created with {len(df)} rows")
                    sys.stdout.flush()
                    
                    # --- STEP 3: Derived Logic ---
                    if 'wallet' in df.columns:
                        mask_base_v = df['wallet'].isin(self.BASE_VAULTS)
                        df['baseVault'] = df['wallet'].where(mask_base_v, None)
                        
                        mask_quote_v = df['wallet'].isin(self.QUOTE_VAULTS)
                        df['quoteVault'] = df['wallet'].where(mask_quote_v, None)
                    
                    if 'mint' in df.columns:
                        mask_base_m = df['mint'].isin(self.BASE_MINTS)
                        df['baseMint'] = df['mint'].where(mask_base_m, None)
                        
                        mask_quote_m = df['mint'].isin(self.QUOTE_MINTS)
                        df['quoteMint'] = df['mint'].where(mask_quote_m, None)
                    
                    # --- STEP 4: Print the Data ---
                    final_cols = [
                        'timestamp',
                        'wallet',
                        'signature',
                        'mint',
                        'pre_balance',
                        'post_balance',
                        'baseVault',
                        'quoteVault',
                        'baseMint',
                        'quoteMint'
                    ]
                    
                    print_df = df[[c for c in final_cols if c in df.columns]]
                    
                    print("-" * 60)
                    print(f"Chunk {chunk_count} | Rows: {len(print_df)} | Timestamp: {ts_val}")
                    print("-" * 60)
                    
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    pd.set_option('display.max_colwidth', None)
                    
                    print(print_df.to_string(index=False))
                    print("-" * 60)
                    sys.stdout.flush()
                    
                except StopIteration:
                    print("  → StopIteration caught, ending stream")
                    sys.stdout.flush()
                    break
                except Exception as inner_e:
                    print(f"  ✗ Error reading chunk: {inner_e}")
                    sys.stdout.flush()
                    import traceback
                    traceback.print_exc()
                    break
            
            print(f"  → Finished reading stream, total chunks: {chunk_count}")
            sys.stdout.flush()
                    
        except Exception as e:
            print(f"✗ OUTER Error during do_put: {e}")
            sys.stdout.flush()
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    location = "grpc+tcp://0.0.0.0:8815"
    server = SolanaFlightServer(location)
    
    try:
        print("=" * 60)
        print("Flight Server Ready - Waiting for connections...")
        print("=" * 60)
        sys.stdout.flush()
        server.serve()
    except KeyboardInterrupt:
        print("\n\nShutting down server...")
        server.shutdown()
