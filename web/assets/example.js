



var exampleQueries = [

    {
        shortname : "Query 1",
        description: " Find out the gene resource related to \"ADRAR\"",
        query:"select ?gene ?p \n"+
"where{ \n\
?gene <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/omim_vocabulary:Gene>. \n\
?gene ?p \"ADRAR\"^^<http://www.w3.org/2001/XMLSchema#string> \n\
}"
    },

    {
        shortname : "Query 2",
        description: "Find out the genes related to diabetes",
        query: "select ?gene ?o1 ?o2 \n\
where{ \n\
?gene <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/omim_vocabulary:Gene>. \n\
?gene ?p1 ?o1. \n\
?o1 <http://bio2rdf.org/omim_vocabulary:x-snomed> ?o2. \n\
<http://bio2rdf.org/pharmgkb:PA446359> <http://bio2rdf.org/pharmgkb_vocabulary:x-snomedct> ?o2 \n\
}"
    },

    {
        shortname : "Query 3",
        description: "Find out the clinical phenotype features, general and specific functions, and omim articles about F8 gene.",
        query: "select *\n\
 where { \n\
?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/omim_vocabulary:Phenotype> .\n\
?s <http://www.w3.org/2000/01/rdf-schema#label> ?o. \n\
?s <http://bio2rdf.org/omim_vocabulary:clinical-features> ?clinicFeature.\n\
?s <http://bio2rdf.org/omim_vocabulary:article> ?article. \n\
?s <http://bio2rdf.org/omim_vocabulary:x-uniprot> ?protein. \n\
?drug <http://bio2rdf.org/drugbank_vocabulary:gene-name> \"F8\"^^<http://www.w3.org/2001/XMLSchema#string>.\n\
?drug <http://bio2rdf.org/drugbank_vocabulary:x-uniprot> ?protein. \n\
?drug <http://bio2rdf.org/drugbank_vocabulary:general-function> ?genFunction. \n\
?drug <http://bio2rdf.org/drugbank_vocabulary:specific-function> ?speFunction \n\
}"
    }


];