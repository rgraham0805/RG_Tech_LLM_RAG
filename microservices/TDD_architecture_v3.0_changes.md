# TDD Architecture v3.0 — Rearchitected with 4x NVIDIA RTX 4500 GPUs

## Change Summary

Rearchitected from v2.0 to consolidate servers with 4x NVIDIA RTX 4500 GPUs
per system, reducing total server count by 75% and upgrading the network fabric
from 400G to 800G backbone.

---

## Side-by-Side Comparison

| Component              | v2.0 (Current)           | v3.0 (Rearchitected)           |
|------------------------|--------------------------|--------------------------------|
| **Pods**               | 4                        | **2**                          |
| **Total Servers**      | 2,720                    | **680** (75% reduction)        |
| **GPUs per Server**    | 0                        | **4x NVIDIA RTX 4500**         |
| **Total GPUs**         | 0                        | **2,720**                      |
| **Total VRAM**         | 0                        | **65,280 GB** (65.3 TB)        |
| **Server Form Factor** | 1U                       | **2U** (GPU clearance)         |
| **Racks per Pod**      | 20                       | 20                             |
| **Servers per Rack**   | 34                       | **17** (2U form factor)        |
| **Total Racks**        | 80                       | **40** (50% reduction)         |
| **Backbone Speed**     | 400G                     | **800G**                       |
| **Spine–Leaf Speed**   | ~100G                    | **400G** (4x upgrade)          |
| **Server–Leaf Speed**  | ~25G                     | **100G** (4x upgrade)          |

---

## Network Equipment Changes

### Border Layer
| Role        | v2.0          | v3.0           | Reason                                    |
|-------------|---------------|----------------|-------------------------------------------|
| Border      | PTX1002       | **PTX10002**   | Higher throughput for 800G uplinks         |
| Optics      | JCO400-QDD-ZR-M | **JCO800-QDD-ZR+** | 800G coherent pluggable OSFP        |
| Pluggable   | QFSD-DD       | **OSFP Module**| Required for 800G optics                  |

### Per-Pod Equipment
| Role        | v2.0              | v3.0                | Count Change       |
|-------------|-------------------|---------------------|-------------------|
| Superspine  | PTX10002          | **PTX10008**        | 1 per pod (same)  |
| Spine       | 4x QFX5130        | **2x QFX5220-64C** | 50% fewer (2 pods)|
| Leaf        | 20x QFX5130       | **20x QFX5130-32CD**| Same per pod      |

### Total Network Equipment
| Device         | v2.0 Total | v3.0 Total | Change    |
|----------------|-----------|-----------|-----------|
| Border routers | 2         | 2         | Same      |
| Superspines    | 4         | **2**     | -50%      |
| Spine switches | 16        | **4**     | -75%      |
| Leaf switches  | 80        | **40**    | -50%      |
| **Total switches** | **102** | **48**  | **-53%** |

---

## GPU Server Specification

Each server contains:

| Component       | Specification                          |
|-----------------|----------------------------------------|
| GPU             | 4x NVIDIA RTX 4500 (Ada Lovelace)     |
| VRAM per GPU    | 24 GB GDDR6X                           |
| TDP per GPU     | 210W                                   |
| Total GPU Power | 840W per server                        |
| Total VRAM      | 96 GB per server                       |
| Form Factor     | 2U rackmount                           |
| NIC             | 100GbE (dual-port for redundancy)      |

---

## Consolidation Rationale

**Why 1/4 the servers?**

With 4x RTX 4500 GPUs per server, each system has 4x the GPU compute
capacity. For GPU-bound AI/ML workloads (LLM inference, RAG embeddings,
training), this means each server handles the work of 4 original servers:

- Original: 2,720 servers × 1 GPU-equivalent = 2,720 units of compute
- New: 680 servers × 4 GPUs = 2,720 units of compute

**Why 2 pods instead of 4?**

With 680 servers at 17 per rack (2U), that's 40 racks total. Two 20-rack
pods maintain the same rack density as v2.0 while halving pod infrastructure
(superspines, spines, border uplinks).

**Why upgrade the network?**

4 GPUs per server generate significantly more network traffic:
- GPU-to-GPU communication across nodes (distributed training)
- Larger model weight transfers
- Higher embedding throughput for RAG pipelines

The 800G backbone and 400G spine-leaf fabric ensure the network doesn't
bottleneck the 4x increase in per-server compute.

---

## Power & Cooling Considerations

| Metric              | v2.0 (est.)       | v3.0 (est.)        |
|---------------------|--------------------|--------------------|
| Server power (avg)  | ~300W × 2,720      | ~1,200W × 680      |
| Total server power  | ~816 kW            | ~816 kW            |
| Network power       | ~102 switches      | ~48 switches        |
| Rack density        | ~10.2 kW/rack      | ~20.4 kW/rack      |
| Cooling requirement | Standard air       | **Enhanced cooling** |

Note: Total server power is roughly equivalent, but rack density doubles.
High-density GPU racks may require rear-door heat exchangers or liquid
cooling for the GPU servers.

---

## Diagram

See `TDD_architecture_v3.0.svg` and `TDD_architecture_v3.0.png` for the
visual architecture diagram.
