% modelpath_mat: input model path, mat format
% modelpath_txt: output model path, txt format
function ModelMat2Txt(modelpath_mat, modelpath_txt)
model = load(modelpath_mat);

text = printStruct('model', model.model, 0);

fid = fopen(modelpath_txt, 'w');
fprintf(fid, '%s', text);
fclose(fid);

function text = printModel(structModelName, structModelValue)
text = [structModelName ' = [\n' ];
% now write hard-coded converting scheme
text = [text, '  ', printNumber('sbin', structModelValue.sbin), ',\n'];
text = [text, '  ', printNumber('interval', structModelValue.interval), ',\n'];
text = [text, '  ', printNumber('numcomponents', structModelValue.components), ',\n'];
text = [text, '  ', printNumber('numblocks', structModelValue.numblocks), ',\n'];
text = [text, '  ', printNumber('thresh', structModelValue.thresh), ',\n'];
text = [text, '  ', printNumber('negfrompos_overlap', structModelValue.negfrompos_overlap), ',\n'];
text = [text, '  ', printArray('blocksizes', structModelValue.blocksizes), ',\n'];
text = [text, '  ', printArray('learnmult', structModelValue.learnmult), ',\n'];
text = [text, '  ', printArray('maxsize', structModelValue.maxsize), ',\n'];
text = [text, '  ', printArray('minsize', structModelValue.minsize), ',\n'];
text = [text, '  ', printCell('rootfilters', structModelValue.rootfilters), ',\n'];
text = [text, '  ', printCell('offsets', structModelValue.offsets), ',\n'];
text = [text, '  ', printCell('lowerbounds', structModelValue.lowerbounds), ',\n'];
text = [text, '  ', printCell('components', structModelValue.components), ',\n'];
text = [text, '  ', printCell('partfilters', structModelValue.partfilters), ',\n'];
text = [text, '  ', printCell('defs', structModelValue.defs), '\n'];
text = [text, ']\n'];

function text = printCell(cellVarName, cellVarValue, shiftwidth)
indent = ' ';
indent = repmat(indent, 1, shiftwidth);
sz = size(cellVarValue);
text = sprintf('%s%s{%s} = {\n', indent, cellVarName, printVector(sz));
valuebuf = [];
for i=1:length(cellVarValue)
    t = cellVarValue{i};
    if iscell(t)
        value = printCell('', t, shiftwidth+2);
    elseif isstruct(t)
        value = printStruct('', t, shiftwidth+2);
    elseif isnumeric(t)
        value = printArray('', t, shiftwidth+2);
    elseif ischar(t)
        value = [indent, '  ', '"', t, '"'];
    end
    valuebuf = sprintf('%s,\n%s', valuebuf, value);
end 
text = sprintf('%s%s\n%s}', text, valuebuf(3:end), indent); 

function text = printStruct(structVarName, structVarValue, shiftwidth)
indent = ' ';
indent = repmat(indent, 1, shiftwidth);
sNames = fieldnames(structVarValue);
text = sprintf('%s%s<%s> = <\n', indent, structVarName, printFields(sNames));
valuebuf = '';
for i=1:numel(sNames)
    t = structVarValue.(sNames{i});
    if iscell(t)
        value = printCell(sNames{i}, t, shiftwidth+2);
    elseif isstruct(t)
        value = printStruct(sNames{i}, t, shiftwidth+2);
    elseif isnumeric(t)
        value = printArray(sNames{i}, t, shiftwidth+2);
    elseif ischar(t)
        value = [indent, '  ', sNames{i}, ' = "', t, '"'];
    else
        continue;
    end
    valuebuf = sprintf('%s,\n%s', valuebuf, value);
end 
text = sprintf('%s%s\n%s>', text, valuebuf(3:end), indent); 

function text = printArray(arrVarName, arrVarValue, shiftwidth)
indent = ' ';
indent = repmat(indent, 1, shiftwidth);
sz = size(arrVarValue);
text = [indent, arrVarName '[' printVector(sz) '] = ['];
vecVarValue = arrVarValue(:);
value = num2str(vecVarValue(1));
for i=2:length(vecVarValue)
    value = [value, ', ', num2str(vecVarValue(i))];
end 
text = [text, value, ']']; 

function text = printNumber(numVarName, numVarValue)
text = [numVarName ' = ' ];
value = num2str(numVarValue);
text = [text, value]; 

function text = printVector(vecVarValue)
text = '';
value = num2str(vecVarValue(1));
for i=2:length(vecVarValue)
    value = [value, ', ', num2str(vecVarValue(i))];
end 
text = [text, value]; 

function text = printFields(cellFields)
text = '';
value = cellFields{1};
for i=2:length(cellFields)
    value = [value, ', ', cellFields{i}];
end 
text = [text, value]; 
