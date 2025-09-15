import json

def load_json_schema(path):
    with open(path, 'r') as file:
        content = file.read()
        return json.loads(content)


def test_bulk_add_raw(man):
    errors = []

    G = man.writeTest()
    G.addJsonSchema(load_json_schema("conformance/graphs/prompt-schema.json"))

    fetchedSchema = G.getSchema()
    len_vertices = len(fetchedSchema['vertices'])
    if len_vertices != 2:
        errors.append(f"incorrect number of vertices in schema {len_vertices} != 2")

    bulkRaw = G.bulkAddRaw()
    bulkRaw.addJson(data={"id": "prompt:0", "resourceType":"Prompt", "text": "Identify the drug-target interactions in the passage given below (along with the interaction type among the following: 'inhibitor', 'agonist', 'modulator', 'activator', 'blocker', 'inducer', 'antagonist', 'cleavage', 'disruption', 'intercalation', 'inactivator', 'bind', 'binder', 'partial agonist', 'cofactor', 'substrate', 'ligand', 'chelator', 'downregulator', 'other', 'antibody', 'other/unknown'):\n\nInhibition of rat brain monoamine oxidase activities by psoralen and isopsoralen: implications for the treatment of affective disorders.        Psoralen and isopsoralen, furocoumarins isolated from the plant Psoralea corylifolia L., were demonstrated to exhibit in vitro inhibitory actions on monoamine oxidase (MAO) activities in rat brain mitochondria, preferentially inhibiting MAO-A activity over MAO-B activity. This inhibition of enzyme activities was found to be dose-dependent and reversible. For MAO-A, the IC50 values are 15.2 +/- 1.3 microM psoralen and 9.0 +/- 0.6 microM isopsoralen. For MAO-B, the IC50 values are 61.8 +/- 4.3 microM psoralen and 12.8 +/- 0.5 microM isopsoralen. Lineweaver-Burk transformation of the inhibition data indicates that inhibition by both psoralen and isopsoralen is non-competitive for MAO-A. The Ki values were calculated to be 14.0 microM for psoralen and 6.5 microM for isopsoralen. On the other hand, inhibition by both psoralen and isopsoralen is competitive for MAO-B. The Ki values were calculated to be 58.1 microM for psoralen and 10.8 microM for isopsoralen. These inhibitory actions of psoralen and isopsoralen on rat brain mitochondrial MAO activities are discussed in relation to their toxicities and their potential applications to treat affective disorders.\n", "responses": ["response:0"]})
    bulkRaw.addJson(data={"id": "prompt:1", "resourceType":"Prompt", "text": "Identify the drug-target interactions in the passage given below (along with the interaction type among the following: 'inhibitor', 'agonist', 'modulator', 'activator', 'blocker', 'inducer', 'antagonist', 'cleavage', 'disruption', 'intercalation', 'inactivator', 'bind', 'binder', 'partial agonist', 'cofactor', 'substrate', 'ligand', 'chelator', 'downregulator', 'other', 'antibody', 'other/unknown'):\n\nSelective inhibitor of Janus tyrosine kinase 3, PNU156804, prolongs allograft survival and acts synergistically with cyclosporine but additively with rapamycin.\tJanus kinase 3 (Jak3) is a cytoplasmic tyrosine (Tyr) kinase associated with the interleukin-2 (IL-2) receptor common gamma chain (gamma(c)) that is activated by multiple T-cell growth factors (TCGFs) such as IL-2, -4, and -7. Using human T cells, it was found that a recently discovered variant of the undecylprodigiosin family of antibiotics, PNU156804, previously shown to inhibit IL-2-induced cell proliferation, also blocks IL-2-mediated Jak3 auto-tyrosine phosphorylation, activation of Jak3 substrates signal transducers and activators of transcription (Stat) 5a and Stat5b, and extracellular regulated kinase 1 (Erk1) and Erk2 (p44/p42). Although PNU156804 displayed similar efficacy in blocking Jak3-dependent T-cell proliferation by IL-2, -4, -7, or -15, it was more than 2-fold less effective in blocking Jak2-mediated cell growth, its most homologous Jak family member. A 14-day alternate-day oral gavage with 40 to 120 mg/kg PNU156804 extended the survival of heart allografts in a dose-dependent fashion. In vivo, PNU156804 acted synergistically with the signal 1 inhibitor cyclosporine A (CsA) and additively with the signal 3 inhibitor rapamycin to block allograft rejection. It is concluded that inhibition of signal 3 alone by targeting Jak3 in combination with a signal 1 inhibitor provides a unique strategy to achieve potent immunosuppression.\n", "responses": ["response:1"]})
    bulkRaw.addJson(data={"id": "response:0", "resourceType":"Response", "text": "\n\nDrug: Psoralen\nTarget: Monoamine oxidase (MAO)\nInteraction Type: Inhibitor\n\nDrug: Isopsoralen\nTarget: Monoamine oxidase (MAO)\nInteraction Type: Inhibitor", "prompt": "prompt:0"})
    bulkRaw.addJson(data={"id": "response:1", "resourceType":"Response", "text": "\n\nDrug-target interactions:\n- PNU156804: inhibitor of Jak3\n- Cyclosporine A: signal 1 inhibitor\n- Rapamycin: signal 3 inhibitor\n\nInteraction types:\n- PNU156804: inhibitor\n- Cyclosporine A: inhibitor\n- Rapamycin: inhibitor/signal transduction inhibitor (exact interaction type not specified in the passage)", "prompt": "prompt:1"})
    err = bulkRaw.execute()

    if err["insertCount"] != 4:
        errors.append(f"Wrong number of inserted vertices {err['insertCount']} != 4")
    if len(err["errors"]) != 0:
        errors.append(f"Wrong number of errors {len(err['errors'])} != 0")

    vertex = G.getVertex("prompt:0")['responses']
    if vertex != ['response:0']:
        errors.append("prompt:0 responses != ['response:0']")

    labels = G.listLabels()
    if not all(item in labels['vertexLabels'] for item in ['Prompt', 'Response']):
        errors.append(f"After insert operations {labels} != expected {{'vertexLabels': ['Condition', 'Patient']}}")

    return errors



def test_bulk_add_raw_validation_error(man):
    errors = []
    G = man.writeTest()
    G.addJsonSchema(load_json_schema("conformance/graphs/prompt-schema.json"))

    bulkRaw = G.bulkAddRaw()
    bulkRaw.addJson(data={"id": "prompt:0", "resourceType":"Prompt", "text": "Identify the drug-target interactions in the passage given below (along with the interaction type among the following: 'inhibitor', 'agonist', 'modulator', 'activator', 'blocker', 'inducer', 'antagonist', 'cleavage', 'disruption', 'intercalation', 'inactivator', 'bind', 'binder', 'partial agonist', 'cofactor', 'substrate', 'ligand', 'chelator', 'downregulator', 'other', 'antibody', 'other/unknown'):\n\nInhibition of rat brain monoamine oxidase activities by psoralen and isopsoralen: implications for the treatment of affective disorders.        Psoralen and isopsoralen, furocoumarins isolated from the plant Psoralea corylifolia L., were demonstrated to exhibit in vitro inhibitory actions on monoamine oxidase (MAO) activities in rat brain mitochondria, preferentially inhibiting MAO-A activity over MAO-B activity. This inhibition of enzyme activities was found to be dose-dependent and reversible. For MAO-A, the IC50 values are 15.2 +/- 1.3 microM psoralen and 9.0 +/- 0.6 microM isopsoralen. For MAO-B, the IC50 values are 61.8 +/- 4.3 microM psoralen and 12.8 +/- 0.5 microM isopsoralen. Lineweaver-Burk transformation of the inhibition data indicates that inhibition by both psoralen and isopsoralen is non-competitive for MAO-A. The Ki values were calculated to be 14.0 microM for psoralen and 6.5 microM for isopsoralen. On the other hand, inhibition by both psoralen and isopsoralen is competitive for MAO-B. The Ki values were calculated to be 58.1 microM for psoralen and 10.8 microM for isopsoralen. These inhibitory actions of psoralen and isopsoralen on rat brain mitochondrial MAO activities are discussed in relation to their toxicities and their potential applications to treat affective disorders.\n", "responses": ["response:0"]})
    bulkRaw.addJson(data={"id": ["prompt:1"], "resourceType":"Prompt", "text": "Identify the drug-target interactions in the passage given below (along with the interaction type among the following: 'inhibitor', 'agonist', 'modulator', 'activator', 'blocker', 'inducer', 'antagonist', 'cleavage', 'disruption', 'intercalation', 'inactivator', 'bind', 'binder', 'partial agonist', 'cofactor', 'substrate', 'ligand', 'chelator', 'downregulator', 'other', 'antibody', 'other/unknown'):\n\nSelective inhibitor of Janus tyrosine kinase 3, PNU156804, prolongs allograft survival and acts synergistically with cyclosporine but additively with rapamycin.\tJanus kinase 3 (Jak3) is a cytoplasmic tyrosine (Tyr) kinase associated with the interleukin-2 (IL-2) receptor common gamma chain (gamma(c)) that is activated by multiple T-cell growth factors (TCGFs) such as IL-2, -4, and -7. Using human T cells, it was found that a recently discovered variant of the undecylprodigiosin family of antibiotics, PNU156804, previously shown to inhibit IL-2-induced cell proliferation, also blocks IL-2-mediated Jak3 auto-tyrosine phosphorylation, activation of Jak3 substrates signal transducers and activators of transcription (Stat) 5a and Stat5b, and extracellular regulated kinase 1 (Erk1) and Erk2 (p44/p42). Although PNU156804 displayed similar efficacy in blocking Jak3-dependent T-cell proliferation by IL-2, -4, -7, or -15, it was more than 2-fold less effective in blocking Jak2-mediated cell growth, its most homologous Jak family member. A 14-day alternate-day oral gavage with 40 to 120 mg/kg PNU156804 extended the survival of heart allografts in a dose-dependent fashion. In vivo, PNU156804 acted synergistically with the signal 1 inhibitor cyclosporine A (CsA) and additively with the signal 3 inhibitor rapamycin to block allograft rejection. It is concluded that inhibition of signal 3 alone by targeting Jak3 in combination with a signal 1 inhibitor provides a unique strategy to achieve potent immunosuppression.\n", "responses": ["response:1"]})
    bulkRaw.addJson(data={"id": "response:0", "resourceType":"Response", "text": "\n\nDrug: Psoralen\nTarget: Monoamine oxidase (MAO)\nInteraction Type: Inhibitor\n\nDrug: Isopsoralen\nTarget: Monoamine oxidase (MAO)\nInteraction Type: Inhibitor", "prompt": "prompt:0"})
    bulkRaw.addJson(data={"id": "response:1", "resourceType":"Response", "text": "\n\nDrug-target interactions:\n- PNU156804: inhibitor of Jak3\n- Cyclosporine A: signal 1 inhibitor\n- Rapamycin: signal 3 inhibitor\n\nInteraction types:\n- PNU156804: inhibitor\n- Cyclosporine A: inhibitor\n- Rapamycin: inhibitor/signal transduction inhibitor (exact interaction type not specified in the passage)", "prompt": "prompt:1"})

    err = bulkRaw.execute()
    if err['insertCount'] != 3 or len(err['errors']) != 1:
        errors.append(f"validation error causes -1 insertCount +1 error: {err['insertCount']} != 3 or {len(err['errors'])} != 1")

    return errors


def test_bulk_add_raw_no_schema(man):
    errors = []

    G = man.writeTest()
    bulkRaw = G.bulkAddRaw()
    bulkRaw.addJson(data={"category":[{"coding":[{"code":"encounter-diagnosis","display":"Encounter Diagnosis","system":"http://terminology.hl7.org/CodeSystem/condition-category"}]}],"clinicalStatus":{"coding":[{"code":"active","system":"http://terminology.hl7.org/CodeSystem/condition-clinical"}]},"code":{"coding":[{"code":"230690007","display":"Stroke","system":"http://snomed.info/sct"}],"text":"Stroke"},"encounter":{"reference":"Encounter/f78a1442-683a-4ea2-adca-161902be19cb"},"id":"838e42fb-a65d-4039-9f83-59c37b1ae889","links":[{"href":"Patient/45c11dad-2b38-4c8e-822e-7abff8a1ee1d","rel":"subject_Patient"}],"meta":{"lastUpdated":"2023-01-26T14:21:56.658+00:00","profile":["http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition"],"source":"#DmW9sueQ4yuQdyA9","versionId":"1"},"onsetDateTime":"2013-08-24T15:40:53-04:00","recordedDate":"2013-08-24T15:40:53-04:00","resourceType":"Condition","subject":{"reference":"Patient/45c11dad-2b38-4c8e-822e-7abff8a1ee1d"},"verificationStatus":{"coding":[{"code":"confirmed","system":"http://terminology.hl7.org/CodeSystem/condition-ver-status"}]}})
    err = bulkRaw.execute()
    if len(err["errors"]) != 1:
        errors.append("No schema was provided so error count should equal 1")

    return []
