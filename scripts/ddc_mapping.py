"""
Maps Dewey Decimal Classification (DDC) subject codes -- present as
"ddc:xxx" keywords on many repository-16 (uni-halle thesis) projects --
onto ISIC Rev. 5 division codes.

DDC identifies *academic subject*, ISIC identifies *economic activity*.
These don't line up perfectly, so this is a best-effort heuristic:
mostly "what industry/field does this research belong to".

Lookup order used by the classifier:
1. Exact 3-digit DDC code (SPECIFIC_DDC_MAP)
2. Hundred-level fallback, e.g. 615 -> 600 (HUNDRED_DDC_MAP)
3. No DDC signal if neither matches
"""

# Exact matches, curated against the DDC codes actually observed in this
# project's data (see: SELECT keyword FROM keywords WHERE keyword LIKE 'ddc:%')
SPECIFIC_DDC_MAP = {
    "000": "S91",  # General works / computer science borderline -> archives/library
    "004": "K62",  # Computer science
    "005": "K62",  # Computer programming
    "006": "K62",  # Special computer methods (AI, etc.)
    "090": "S91",  # Manuscripts & rare books
    "150": "N72",  # Psychology
    "300": "N72",  # Social sciences (general)
    "330": "N72",  # Economics
    "340": "N69",  # Law
    "345": "N69",  # Criminal law
    "370": "Q85",  # Education
    "510": "N72",  # Mathematics
    "512": "N72",  # Algebra
    "530": "N72",  # Physics
    "540": "N72",  # Chemistry
    "550": "N72",  # Earth sciences
    "556": "N72",  # Earth sciences (Europe)
    "570": "N72",  # Life sciences / biology
    "571": "N72",  # Physiology & related
    "572": "N72",  # Biochemistry
    "573": "N72",  # Physical anthropology / human biology
    "575": "N72",  # Evolution & genetics
    "576": "N72",  # Genetics & microbiology
    "579": "N72",  # Microorganisms, fungi, algae
    "580": "N72",  # Botany
    "581": "N72",  # Botany (specific)
    "590": "N72",  # Zoology
    "595": "N72",  # Invertebrates
    "610": "R86",  # Medicine & health
    "611": "R86",  # Human anatomy
    "612": "R86",  # Human physiology
    "615": "R86",  # Pharmacology & therapeutics
    "616": "R86",  # Diseases
    "618": "R86",  # Gynecology, obstetrics, pediatrics
    "630": "A01",  # Agriculture
    "633": "A01",  # Field crops
    "780": "S90",  # Music
    "900": "S91",  # History & geography (general)
    "940": "S91",  # European history
    "943": "S91",  # German history
}

# Fallback: DDC "hundred" class -> ISIC division, used when the exact
# 3-digit code isn't in SPECIFIC_DDC_MAP above.
HUNDRED_DDC_MAP = {
    "000": "S91",
    "100": "N72",
    "200": "S91",
    "300": "N72",
    "400": "S91",
    "500": "N72",
    "600": "N72",
    "700": "S90",
    "800": "S91",
    "900": "S91",
}


def ddc_to_isic(ddc_code: str) -> str | None:
    """ddc_code like '610' or 'ddc:610' -> ISIC division code, or None."""
    code = ddc_code.replace("ddc:", "").strip()
    if not code.isdigit():
        return None
    if code in SPECIFIC_DDC_MAP:
        return SPECIFIC_DDC_MAP[code]
    hundred = code[0] + "00" if len(code) == 3 else None
    if hundred and hundred in HUNDRED_DDC_MAP:
        return HUNDRED_DDC_MAP[hundred]
    return None
