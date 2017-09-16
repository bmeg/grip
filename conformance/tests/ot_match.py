



def test_count(O):
    errors = []

    O.addVertex("gene:BRAF", {"symbol" : "BRAF"})
    O.addVertex("gene:TP53", {"symbol" : "TP53"})
    O.addVertex("gene:MDM4", {"symbol" : "MDM4"})
    O.addVertex("variant:BRAF", {"type" : "SNP"})
    O.addVertex("variant:TP53", {"type" : "SNP"})

    O.addEdge("variant:BRAF", "gene:BRAF", "variantInGene")
    O.addEdge("variant:TP53", "gene:TP53", "variantInGene")
    
    O.addEdge("variant:BRAF", "sample:1", "variantInBiosample")
    O.addEdge("variant:TP53", "sample:2", "variantInBiosample")
    
    
    O.query().match([
     O.mark("gene").has("symbol", "BRAF"),
     O.mark("gene").incoming("variantInGene").outgoing("variantInBiosample").mark("sample")
    ]).select(["gene", "sample"])