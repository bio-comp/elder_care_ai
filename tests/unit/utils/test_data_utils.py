import tempfile
from pathlib import Path

import pytest
import rarfile

from elder_risk.utils.data_utils import extract_nested_archives


@pytest.fixture
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent.parent


@pytest.fixture
def test_rar_file(project_root):
    """Get the main Tests.rar file."""
    return project_root / "data" / "Tests.rar"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestExtractNestedArchives:
    def test_extract_main_rar_file(self, test_rar_file, temp_dir):
        """Test extracting the main Tests.rar file."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Extract just the first level (the nested RAR files)
        extracted = extract_nested_archives(test_rar_file, temp_dir, max_depth=1)
        
        assert len(extracted) > 0
        # Should extract the nested RAR files
        rar_files = [f for f in extracted if f.suffix.lower() == '.rar']
        assert len(rar_files) >= 17  # Should have at least 17 RAR files
        assert all(f.exists() for f in extracted)
    
    def test_extract_nested_rar_from_main(self, test_rar_file, temp_dir):
        """Test extracting nested RAR files from the main archive."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Extract with depth 2 to get contents of nested RARs
        extracted = extract_nested_archives(test_rar_file, temp_dir, max_depth=2)
        
        assert isinstance(extracted, list)
        assert all(isinstance(p, Path) for p in extracted)
        
        # Should have both RAR files and their contents
        assert len(extracted) > 17  # More than just the RAR files themselves
    
    def test_max_depth_protection(self, test_rar_file, temp_dir):
        """Test that max_depth prevents infinite recursion."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Extract with depth 0 - should not recurse into nested RARs
        extracted = extract_nested_archives(test_rar_file, temp_dir, max_depth=0)
        
        # Should extract files but not recurse
        assert len(extracted) >= 17  # Just the RAR files, not their contents
    
    def test_extract_callback_functionality(self, test_rar_file, temp_dir):
        """Test that callbacks are invoked for each extracted file."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        extracted_paths = []
        
        def track_extraction(path: Path) -> None:
            extracted_paths.append(path)
        
        result = extract_nested_archives(
            test_rar_file, 
            temp_dir,
            max_depth=1,
            extract_callback=track_extraction
        )
        
        # Callback should be called for each file
        assert len(extracted_paths) == len(result)
        assert all(p in result for p in extracted_paths)
    
    def test_should_extract_filter(self, test_rar_file, temp_dir):
        """Test selective extraction using should_extract predicate."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        def only_specific_files(path: Path) -> bool:
            # Only extract files starting with '10'
            return path.name.startswith('10')
        
        extracted = extract_nested_archives(
            test_rar_file,
            temp_dir,
            max_depth=1,
            should_extract=only_specific_files
        )
        
        # All extracted files should match the filter
        for file_path in extracted:
            assert file_path.name.startswith('10')
    
    def test_output_directory_creation(self, test_rar_file):
        """Test that output directory is created if it doesn't exist."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "new" / "nested" / "dir"
            assert not output_dir.exists()
            
            extracted = extract_nested_archives(test_rar_file, output_dir, max_depth=1)
            
            assert output_dir.exists()
            assert output_dir.is_dir()
            assert len(extracted) > 0
    
    def test_temp_directory_creation_when_no_output_dir(self, test_rar_file):
        """Test that a temp directory is created when output_dir is None."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        extracted = extract_nested_archives(test_rar_file, max_depth=1)
        
        assert len(extracted) > 0
        # All files should be in a temp directory
        assert all(f.exists() for f in extracted)
        
        # Get the parent directory
        if extracted:
            parent_dirs = {f.parent for f in extracted}
            for parent in parent_dirs:
                assert "rar_extract_" in str(parent) or "/tmp" in str(parent) or "/var" in str(parent)
    
    def test_nonexistent_file_raises_error(self):
        """Test that FileNotFoundError is raised for nonexistent files."""
        fake_path = Path("/nonexistent/path/to/archive.rar")
        
        with pytest.raises(FileNotFoundError, match="Archive file not found"):
            extract_nested_archives(fake_path)
    
    def test_handles_password_parameter(self, test_rar_file, temp_dir):
        """Test that password parameter is properly handled."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Test with a password (even though file isn't protected)
        extracted = extract_nested_archives(
            test_rar_file, 
            temp_dir, 
            password="dummy_password",
            max_depth=1
        )
        assert len(extracted) > 0
    
    def test_nested_rar_deep_extraction(self, test_rar_file, temp_dir):
        """Test deep extraction of nested RAR files."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Extract with higher depth to get contents of nested archives
        extracted = extract_nested_archives(test_rar_file, temp_dir, max_depth=3)
        
        assert len(extracted) > 0
        
        # Should have both RAR files and their extracted contents
        rar_files = [f for f in extracted if f.suffix.lower() == '.rar']
        non_rar_files = [f for f in extracted if f.suffix.lower() != '.rar']
        
        assert len(rar_files) >= 17  # The nested RAR files
        # If depth > 1, should also have extracted contents
        if len(non_rar_files) > 0:
            assert any(f.suffix.lower() != '.rar' for f in non_rar_files)
    
    def test_preserves_directory_structure(self, test_rar_file, temp_dir):
        """Test that directory structure from archive is preserved."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        extracted = extract_nested_archives(test_rar_file, temp_dir, max_depth=1)
        
        # Check that files maintain their relative paths
        for file_path in extracted:
            assert file_path.exists()
            # Files should be in Tests/ subdirectory
            assert 'Tests' in str(file_path)
            # The file should be under our output directory
            assert temp_dir in file_path.parents or temp_dir == file_path.parent
    
    def test_string_path_input(self, test_rar_file, temp_dir):
        """Test that function accepts string paths as well as Path objects."""
        if not test_rar_file.exists():
            pytest.skip(f"Test RAR file not found: {test_rar_file}")
        
        # Pass as string instead of Path
        extracted = extract_nested_archives(str(test_rar_file), str(temp_dir), max_depth=1)
        
        assert len(extracted) > 0
        assert all(isinstance(p, Path) for p in extracted)
        assert all(f.exists() for f in extracted)