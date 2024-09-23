namespace Snowflake.Data.Core.Converter
{
    internal class StructurePath
    {
        private readonly string _path;

        public StructurePath() : this("$")
        {
        }

        private StructurePath(string path)
        {
            _path = path;
        }

        public StructurePath WithPropertyIndex(int propertyIndex)
        {
            return new StructurePath($"{_path}[{propertyIndex}]");
        }

        public StructurePath WithArrayIndex(int arrayIndex)
        {
            return new StructurePath($"{_path}[{arrayIndex}]");
        }

        public StructurePath WithMapIndex(int mapIndex)
        {
            return new StructurePath($"{_path}[{mapIndex}]");
        }

        public override string ToString()
        {
            return _path;
        }
    }
}
