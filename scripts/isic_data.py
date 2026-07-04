"""
ISIC Rev. 5 division data (Section + Division level, per project brief p.24).

Each division has:
- code: section+division code, e.g. "N72"
- number: the two-digit division number, e.g. "72"
- title: the official division title
- keywords: extra terms (beyond the title's own words) that indicate this
  division in academic/research text. These are hand-curated for divisions
  that commonly show up in qualitative/academic research, and left minimal
  for divisions unlikely to appear (e.g. gambling, tobacco manufacture).

The title words themselves are automatically added as keywords too
(see build_lexicon() at the bottom), so you don't need to repeat them here.
"""

import re

# (code, number, title)
DIVISIONS_RAW = [
    ("A01", "01", "Crop and animal production, hunting and related service activities"),
    ("A02", "02", "Forestry and logging"),
    ("A03", "03", "Fishing and aquaculture"),
    ("B05", "05", "Mining of coal and lignite"),
    ("B06", "06", "Extraction of crude petroleum and natural gas"),
    ("B07", "07", "Mining of metal ores"),
    ("B08", "08", "Other mining and quarrying"),
    ("B09", "09", "Mining support service activities"),
    ("C10", "10", "Manufacture of food products"),
    ("C11", "11", "Manufacture of beverages"),
    ("C12", "12", "Manufacture of tobacco products"),
    ("C13", "13", "Manufacture of textiles"),
    ("C14", "14", "Manufacture of wearing apparel"),
    ("C15", "15", "Manufacture of leather and related products"),
    ("C16", "16", "Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials"),
    ("C17", "17", "Manufacture of paper and paper products"),
    ("C18", "18", "Printing and reproduction of recorded media"),
    ("C19", "19", "Manufacture of coke and refined petroleum products"),
    ("C20", "20", "Manufacture of chemicals and chemical products"),
    ("C21", "21", "Manufacture of basic pharmaceutical products and pharmaceutical preparations"),
    ("C22", "22", "Manufacture of rubber and plastic products"),
    ("C23", "23", "Manufacture of other non-metallic mineral products"),
    ("C24", "24", "Manufacture of basic metals"),
    ("C25", "25", "Manufacture of fabricated metal products, except machinery and equipment"),
    ("C26", "26", "Manufacture of computer, electronic and optical products"),
    ("C27", "27", "Manufacture of electrical equipment"),
    ("C28", "28", "Manufacture of machinery and equipment n.e.c."),
    ("C29", "29", "Manufacture of motor vehicles, trailers and semi-trailers"),
    ("C30", "30", "Manufacture of other transport equipment"),
    ("C31", "31", "Manufacture of furniture"),
    ("C32", "32", "Other manufacturing"),
    ("C33", "33", "Repair, maintenance and installation of machinery and equipment"),
    ("D35", "35", "Electricity, gas, steam and air conditioning supply"),
    ("E36", "36", "Water collection, treatment and supply"),
    ("E37", "37", "Sewerage"),
    ("E38", "38", "Waste collection, treatment and disposal, and recovery activities"),
    ("E39", "39", "Remediation and other waste management service activities"),
    ("F41", "41", "Construction of residential and non-residential buildings"),
    ("F42", "42", "Civil engineering"),
    ("F43", "43", "Specialized construction activities"),
    ("G46", "46", "Wholesale trade"),
    ("G47", "47", "Retail trade"),
    ("H49", "49", "Land transport and transport via pipelines"),
    ("H50", "50", "Water transport"),
    ("H51", "51", "Air transport"),
    ("H52", "52", "Warehousing and support activities for transportation"),
    ("H53", "53", "Postal and courier activities"),
    ("I55", "55", "Accommodation"),
    ("I56", "56", "Food and beverage service activities"),
    ("J58", "58", "Publishing activities"),
    ("J59", "59", "Motion picture, video and television programme production, sound recording and music publishing activities"),
    ("J60", "60", "Programming, broadcasting, news agency and other content distribution activities"),
    ("K61", "61", "Telecommunications"),
    ("K62", "62", "Computer programming, consultancy and related activities"),
    ("K63", "63", "Computing infrastructure, data processing, hosting, and other information service activities"),
    ("L64", "64", "Financial service activities, except insurance and pension funding"),
    ("L65", "65", "Insurance, reinsurance and pension funding, except compulsory social security"),
    ("L66", "66", "Activities auxiliary to financial service and insurance activities"),
    ("M68", "68", "Real estate activities"),
    ("N69", "69", "Legal and accounting activities"),
    ("N70", "70", "Activities of head offices; management consultancy activities"),
    ("N71", "71", "Architectural and engineering activities; technical testing and analysis"),
    ("N72", "72", "Scientific research and development"),
    ("N73", "73", "Activities of advertising, market research and public relations"),
    ("N74", "74", "Other professional, scientific and technical activities"),
    ("N75", "75", "Veterinary activities"),
    ("O77", "77", "Rental and leasing activities"),
    ("O78", "78", "Employment activities"),
    ("O79", "79", "Travel agency, tour operator, and other travel related activities"),
    ("O80", "80", "Investigation and security activities"),
    ("O81", "81", "Services to buildings and landscape activities"),
    ("O82", "82", "Office administrative, office support and other business support activities"),
    ("P84", "84", "Public administration and defence; compulsory social security"),
    ("Q85", "85", "Education"),
    ("R86", "86", "Human health activities"),
    ("R87", "87", "Residential care activities"),
    ("R88", "88", "Social work activities without accommodation"),
    ("S90", "90", "Arts creation and performing arts activities"),
    ("S91", "91", "Library, archives, museum and other cultural activities"),
    ("S92", "92", "Gambling and betting activities"),
    ("S93", "93", "Sports activities and amusement and recreation activities"),
    ("T94", "94", "Activities of membership organizations"),
    ("T95", "95", "Repair and maintenance of computers, personal and household goods, and motor vehicles and motorcycles"),
    ("T96", "96", "Personal service activities"),
    ("U97", "97", "Activities of households as employers of domestic personnel"),
    ("U98", "98", "Undifferentiated goods- and services-producing activities of private households for own use"),
    ("V99", "99", "Activities of extraterritorial organizations and bodies"),
]

# Manually curated extra keywords/synonyms per division code, focused on
# terms likely to appear in academic/research titles & abstracts.
EXTRA_KEYWORDS = {
    "A01": ["agriculture", "farming", "crop", "livestock", "arable", "horticulture", "agronomy"],
    "N72": ["research", "scientific research", "systematic literature review", "slr",
            "empirical study", "thesis", "dissertation", "doctoral"],
    "K62": ["software", "programming", "algorithm", "source code", "api",
            "machine learning", "artificial intelligence", "computer science",
            "app development", "programming language"],
    "K63": ["data processing", "cloud", "hosting", "database", "data hosting"],
    "R86": ["health", "medicine", "medical", "clinical", "patient", "disease",
            "palliative care", "nursing", "healthcare", "hospital", "therapy",
            "diagnosis", "treatment", "psychiatry", "epidemiology"],
    "R87": ["residential care", "nursing home", "elderly care", "care home"],
    "R88": ["social work", "welfare", "counselling", "child protection"],
    "Q85": ["education", "teaching", "learning", "pedagogy", "school", "curriculum",
            "student", "university", "training", "didactics"],
    "N69": ["law", "legal", "jurisprudence", "legislation", "court", "regulation",
            "compliance", "attorney", "contract law"],
    "N71": ["engineering", "architecture", "structural", "technical testing",
            "civil engineering design"],
    "S90": ["art", "music", "performing arts", "theatre", "dance", "painting",
            "sculpture", "creative practice"],
    "S91": ["library", "archive", "museum", "cultural heritage", "curation"],
    "J58": ["publishing", "book", "journal", "editorial"],
    "J59": ["film", "video production", "television", "cinema", "documentary"],
    "K61": ["telecommunications", "network", "wireless", "mobile network"],
    "C20": ["chemistry", "chemical", "chemical engineering", "polymer"],
    "C21": ["pharmaceutical", "pharmacology", "drug development", "pharmacy"],
    "P84": ["public administration", "government", "policy", "governance",
            "public sector", "defence", "military"],
    "L64": ["finance", "banking", "financial services", "investment"],
    "L65": ["insurance", "pension"],
    "M68": ["real estate", "property", "housing market"],
    "H49": ["transport", "logistics", "traffic", "mobility", "railway"],
    "F42": ["civil engineering", "infrastructure", "construction"],
    "E38": ["waste management", "recycling", "environmental management"],
    "T94": ["ngo", "non-governmental organization", "advocacy", "membership organization"],
    "N73": ["advertising", "marketing", "market research", "public relations"],
    "N70": ["management", "consultancy", "business strategy", "organizational management"],
    "O78": ["employment", "human resources", "labor market", "recruitment"],
}

_STOPWORDS = {
    "and", "of", "the", "or", "except", "for", "n.e.c.", "n.e.c", "other",
    "activities", "related", "service", "services", "products", "manufacture",
}


def _tokenize_title(title: str) -> list[str]:
    words = re.findall(r"[a-zA-Z]+", title.lower())
    return [w for w in words if w not in _STOPWORDS and len(w) > 2]


def build_lexicon() -> dict:
    """Returns {code: {"number": str, "title": str, "keywords": set[str]}}"""
    lexicon = {}
    for code, number, title in DIVISIONS_RAW:
        keywords = set(_tokenize_title(title))
        for extra in EXTRA_KEYWORDS.get(code, []):
            keywords.add(extra.lower())
        lexicon[code] = {"number": number, "title": title, "keywords": keywords}
    return lexicon


DIVISIONS = build_lexicon()
