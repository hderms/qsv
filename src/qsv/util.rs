use std::path::Path;

pub fn remove_extension(p0: &Path) -> Option<String> {
    let file_name = p0.file_name()?;
    let file_str = file_name.to_str()?;
    let mut split = file_str.split('.');
    split.next().map(String::from)
}
pub fn sanitize(str: Option<String>) -> Option<String> {
    str.map(|s| s.replace(" ", "_"))
}
