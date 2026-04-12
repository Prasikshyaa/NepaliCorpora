"""
Fixed namespace detection - uses binary mode for lxml.
"""

import bz2
import re
from pathlib import Path
from lxml import etree
from io import TextIOWrapper

def detect_wikipedia_namespace(dump_path: Path) -> str:
    """
    Auto-detect the XML namespace from a Wikipedia dump file.
    
    Args:
        dump_path: Path to .xml.bz2 dump
        
    Returns:
        Namespace string like "{http://www.mediawiki.org/xml/export-0.11/}"
    """
    print(f"Detecting XML namespace from: {dump_path}")
    
    # Open in text mode just to read the header
    with bz2.open(dump_path, 'rt', encoding='utf-8') as xml_file:
        chunk = xml_file.read(5000)
        
        # Look for xmlns="..."
        match = re.search(r'xmlns="([^"]+)"', chunk)
        if match:
            namespace_url = match.group(1)
            namespace = f"{{{namespace_url}}}"
            print(f"  ✓ Detected namespace: {namespace}")
            return namespace
        else:
            print("  ! Could not detect namespace")
            return None


def test_namespace(dump_path: Path, namespace: str) -> int:
    """
    Test if a namespace works by counting pages.
    
    CRITICAL: Must open bz2 file in BINARY mode for lxml.
    
    Args:
        dump_path: Path to .xml.bz2 dump
        namespace: Namespace to test
        
    Returns:
        Number of pages found (stops at 10 for speed)
    """
    # BINARY mode for lxml!
    with bz2.open(dump_path, 'rb') as xml_file:
        try:
            context = etree.iterparse(
                xml_file,
                events=('end',),
                tag=f'{namespace}page'
            )
            
            count = 0
            sample_titles = []
            
            for event, page in context:
                count += 1
                
                # Get title for verification
                title_elem = page.find(f'{namespace}title')
                if title_elem is not None and title_elem.text:
                    sample_titles.append(title_elem.text)
                
                # Clean up
                page.clear()
                while page.getprevious() is not None:
                    del page.getparent()[0]
                
                if count >= 10:
                    break
            
            del context
            
            if count > 0 and sample_titles:
                print(f"  Sample titles: {sample_titles[:3]}")
            
            return count
            
        except Exception as e:
            print(f"  Error: {e}")
            return 0


if __name__ == "__main__":
    # Test with your dump
    dump_path = Path(r"C:\Nepali_corpus_Project\data\raw\wikipedia\latest\newiki-latest-pages-articles.xml.bz2")
    
    if not dump_path.exists():
        print(f"ERROR: Dump file not found: {dump_path}")
        exit(1)
    
    file_size = dump_path.stat().st_size / (1024 * 1024)
    
    print("="*80)
    print("NAMESPACE DETECTION TEST")
    print("="*80)
    print(f"File: {dump_path.name}")
    print(f"Size: {file_size:.1f} MB")
    print()
    
    # Detect namespace
    detected_ns = detect_wikipedia_namespace(dump_path)
    
    if detected_ns:
        # Test it
        print(f"\nTesting detected namespace: {detected_ns}")
        count = test_namespace(dump_path, detected_ns)
        print(f"  ✓ Found {count} pages")
        
        if count > 0:
            print(f"\n{'='*80}")
            print("SUCCESS! Update your extractor:")
            print(f"{'='*80}")
            print(f"In extract_wiki_articles.py, change line 28 from:")
            print(f'  WIKI_NS = "{{http://www.mediawiki.org/xml/export-0.10/}}"')
            print(f"To:")
            print(f'  WIKI_NS = "{detected_ns}"')
            print()
            print("Also ensure line 330 uses BINARY mode:")
            print('  with bz2.open(dump_path, "rb") as xml_file:')
            print('  # NOT: with bz2.open(dump_path, "rt", encoding="utf-8")')
            print(f"{'='*80}")
        else:
            print("\n⚠ Detected namespace found 0 pages. Trying alternatives...")
    
    # Try alternatives if detection failed
    if not detected_ns or count == 0:
        alternatives = [
            "{http://www.mediawiki.org/xml/export-0.11/}",
            "{http://www.mediawiki.org/xml/export-0.10/}",
            "{http://www.mediawiki.org/xml/export-0.12/}",
        ]
        
        print("\nTrying common namespaces...")
        for ns in alternatives:
            print(f"\nTrying: {ns}")
            count = test_namespace(dump_path, ns)
            print(f"  Found {count} pages")
            
            if count > 0:
                print(f"\n{'='*80}")
                print("SUCCESS! Update your extractor:")
                print(f"{'='*80}")
                print(f'WIKI_NS = "{ns}"')
                print(f"{'='*80}")
                break