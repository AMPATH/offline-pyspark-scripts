def get_drug_mappings():
    return {    
            "625": { "mapped_to_ids": "625", "name": "STAVUDINE" },
            "628": { "mapped_to_ids": "628", "name": "LAMIVUDINE" },
            "630" :{ "mapped_to_ids": "628;797", "name": "ZIDOVUDINE AND LAMIVUDINE" },
            "631": { "mapped_to_ids": "631", "name": "NEVIRAPINE" },
            "633": { "mapped_to_ids": "633", "name": "EFAVIRENZ" },
            "635": { "mapped_to_ids": "635", "name": "NELFINAVIR" },
            "749": { "mapped_to_ids": "749", "name": "INDINAVIR" },
            "791": { "mapped_to_ids": "791", "name": "EMTRICITABINE" },
            "792": { "mapped_to_ids": "625;628;631", "name": "STAVUDINE LAMIVUDINE AND NEVIRAPINE" },
            "794": { "mapped_to_ids": "794", "name": "LOPINAVIR AND RITONAVIR" },
            "795": { "mapped_to_ids": "795", "name": "RITONAVIR" },
            "796": { "mapped_to_ids": "796", "name": "DIDANOSINE" },
            "797": { "mapped_to_ids": "797", "name": "ZIDOVUDINE" },
            "802": { "mapped_to_ids": "802", "name": "TENOFOVIR" },
            "814": { "mapped_to_ids": "814", "name": "ABACAVIR" },
            "817": { "mapped_to_ids": "628;797;814", "name": "ABACAVIR LAMIVUDINE AND ZIDOVUDINE" },
            "1065": { "mapped_to_ids": "1065", "name": "YES" },
            "1066": { "mapped_to_ids": "1066", "name": "NO" },
            "1107": { "mapped_to_ids": "1107", "name": "NONE" },
            "1400": { "mapped_to_ids": "628;802", "name": "LAMIVUDINE AND TENOFOVIR" },
            "5424": { "mapped_to_ids": "5424", "name": "OTHER ANTIRETROVIRAL DRUG" },
            "5811": { "mapped_to_ids": "5811", "name": "UNKNOWN ANTIRETROVIRAL DRUG" },
            "6156": { "mapped_to_ids": "6156", "name": "RALTEGRAVIR" },
            "6157": { "mapped_to_ids": "6157", "name": "DARUNAVIR" },
            "6158": { "mapped_to_ids": "6158", "name": "ETRAVIRINE" },
            "6159": { "mapped_to_ids": "6159", "name": "ATAZANAVIR" },
            "6160": { "mapped_to_ids": "795;6159", "name": "ATAZANAVIR AND RITONAVIR" },
            "6180": { "mapped_to_ids": "791;802", "name": "EMTRICITABINE AND TENOFOVIR" },
            "6467": { "mapped_to_ids": "628;631;633", "name": "NEVIRAPINE LAMIVUDINE AND ZIDOVUDINE" },
            "6679": { "mapped_to_ids": "628;814", "name": "ABACAVIR AND LAMIVUDINE" },
            "6964": { "mapped_to_ids": "628;633;802", "name": "TENOFOVIR AND LAMIVUDINE AND EFAVIRENZ" },
            "6965": { "mapped_to_ids": "625;628", "name": "LAMIVUDINE AND STAVUDINE" },
            "9435": { "mapped_to_ids": "9435", "name": "EVIPLERA" },
            "9759": { "mapped_to_ids": "9759", "name": "DOLUTEGRAVIR" },
            "9026": { "mapped_to_ids": "9026", "name": "LOPINAVIR" }
            }


def get_drug_names(drug_ids):
    if(drug_ids is not None):
        if(type(drug_ids) == 'str'):
            drug_ids_array = drug_ids.split(' ## ')
        else:
            drug_ids_array = drug_ids                             
        drug_mappings = get_drug_mappings()
        drug_names = ''
        for drug_id in drug_ids_array:
            try:
                drug_mapping = drug_mappings[drug_id]
            except:
                drug_mapping = None
            if(drug_mapping is not None):
                drug_name = drug_mapping['name']
                drug_names = drug_names + drug_name + ','
        return drug_names
    else:
        return None