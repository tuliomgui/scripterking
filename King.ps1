# Script parameters
param(
    [string]$Hostname = "",
    [string]$Port = "",
    [string]$Login = "",
    [string]$Password = ""
)

# Ensure HashSet is available
Add-Type -TypeDefinition @"
using System.Collections.Generic;
"@

enum TableSearchStates {
    Searching
    FKFound
    ReferenceFound
    TableFound
}

enum OutputFileState {
    DropCreateInsert
    WaitingSeparator
    SeparatorFound
    Dependencies
    CloseIntermediaryFile
}

$global:hostname = ""
$global:port = ""
$global:login = ""
$global:pass = ""

$global:ChunckSize = 150MB
$global:OutputWriter = $null
$global:ShouldWriteFile = $true
$global:CurrFileSize = $null
$global:CurrFileIndex = 0

$global:DCIPath = "DCI"
$global:RefsPath = "Refs"
$global:IsRefsInitiated = $false
$global:DatabaseName = $null

$global:ExecutionOrder = @("Schema", $global:DCIPath, $global:RefsPath, "View", "UserDefinedFunction", "StoredProcedure", "Users")

$global:CurrentState = [TableSearchStates]::Searching
$global:OutputState = [OutputFileState]::DropCreateInsert
$global:AllTables = [System.Collections.ArrayList]::new()
$global:ReferencedTablesList = [System.Collections.ArrayList]::new()
$global:TablesMap = [System.Collections.Hashtable]::new()
$global:TableReferencedByMap = [System.Collections.Hashtable]::new()
$global:ChunckedTables = [System.Collections.Hashtable]::new()

function GetTableSqlFiles {
    return Get-ChildItem -Path "." -Recurse -File -Filter "*Table.sql" -ErrorAction SilentlyContinue
}

function SearchForeignKey {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'FOREIGN KEY') {
        $global:CurrentState = [TableSearchStates]::FKFound
    }
}

function SearchReference {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES') {
        $global:CurrentState = [TableSearchStates]::ReferenceFound
    }
}

function ExtractReferencedTable {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentLine
    )
    if ($CurrentLine -match 'REFERENCES\s+\[?\w+\]?\.\[?(\w+)\]') {
        $ReferencedTable = $matches[1]
        $global:CurrentState = [TableSearchStates]::TableFound
        return $ReferencedTable
    }
    return $null
}

function AddTableReferenceControl {
    param (
        [Parameter(Mandatory = $true)][string]$CurrentTableName,
        [Parameter(Mandatory = $false)][string]$ReferencedTableName
    )
    # Add table to the list of all tables
    if (-not ($global:AllTables -contains $CurrentTableName)) {
        $global:AllTables.Add($CurrentTableName) | Out-Null
    }
    if ($CurrentTableName -eq $ReferencedTableName) {
        Write-Host "Current table name is the same as referenced table name. Ignoring $CurrentTableName as a needed reference" -ForegroundColor Red
        return
    }
    if ([string]::IsNullOrWhiteSpace($ReferencedTableName)) {
        return
    }
    # Add control of the table reference
    if ($global:TablesMap.ContainsKey($CurrentTableName)) {
        $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
    }
    else {
        $global:TablesMap.Add($CurrentTableName, [System.Collections.Generic.HashSet[string]]::new())
        $global:TablesMap[$CurrentTableName].Add($ReferencedTableName) | Out-Null
    }
    # Add control of the referenced by tables
    if ($global:TableReferencedByMap.ContainsKey($ReferencedTableName)) {
        $global:TableReferencedByMap[$ReferencedTableName].Add($CurrentTableName) | Out-Null
    }
    else {
        $global:TableReferencedByMap.Add($ReferencedTableName, [System.Collections.Generic.HashSet[string]]::new())
        $global:TableReferencedByMap[$ReferencedTableName].Add($CurrentTableName) | Out-Null
    }
}

function AddReferenceOnLevel {
    param (
        [Parameter(Mandatory = $true)][string]$TableName,
        [Parameter(Mandatory = $true)][int]$Level
    )
    if ($global:ReferencedTablesList.Count -le $Level) {
        foreach ($i in $global:ReferencedTablesList.Count..$Level) {
            $global:ReferencedTablesList.Add([System.Collections.ArrayList]::new()) | Out-Null
        }
    }
    $global:ReferencedTablesList[$Level].add($TableName) | Out-Null
}

function GetTableReferenceLevel {
    param (
        [Parameter(Mandatory = $true)][Int16]$Level,
        [Parameter(Mandatory = $true)][string]$TableName
    )
    if ($global:TableReferencedByMap.ContainsKey($TableName)) {
        $HighestLevel = $Level
        foreach ($Table in $global:TableReferencedByMap[$TableName]) {
            $NewLevel = GetTableReferenceLevel -Level ($Level + 1) -TableName $Table
            if ($NewLevel -gt $HighestLevel) {
                $HighestLevel = $NewLevel
            }
        }
        $Level = $HighestLevel
    }
    else {
        return $Level
    }
    return $Level
}

function GetTableNameFromFileName {
    param (
        [Parameter(Mandatory = $true)][string]$FileName
    )
    
    if ($FileName -match '^\w+\.(\w+)\.\w+\.sql$') {
        return $matches[1]
    }
    Write-Host "No match found for table name in file name." -ForegroundColor Red
    exit 101
}

function ProcessTableReferenceState {
    param (
        [Parameter(Mandatory = $true)][string]$Line
    )
    $ReferencedTable = $null
    if ($global:CurrentState -eq [TableSearchStates]::Searching) {
        SearchForeignKey -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::FKFound) {
        SearchReference -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::ReferenceFound) {
        $ReferencedTable = ExtractReferencedTable -CurrentLine $Line
    }
    if ($global:CurrentState -eq [TableSearchStates]::TableFound) {
        if ($null -eq $ReferencedTable) {
            Write-Host "Referenced table is null. Exiting." -ForegroundColor Red
            exit 100
        }
        AddTableReferenceControl -CurrentTableName $CurrentTableName -ReferencedTableName $ReferencedTable
        $global:CurrentState = [TableSearchStates]::Searching
    }
}

function ProcessOutputFileState {
    param (
        [Parameter(Mandatory = $true)][string]$Line,
        [Parameter(Mandatory = $true)][string]$TableName
    )
    # Salva o nome do banco de dados para uso posterior
    if ([string]::IsNullOrWhiteSpace($global:DatabaseName) -and $Line -match 'USE\s+(\[?\w+\]?)') {
        $global:DatabaseName = $matches[1]
    }
    # Vai lendo o arquivo atual até achar a instrução 'CREATE TABLE', quando achar muda para o próximo estado
    if ($global:OutputState -eq [OutputFileState]::DropCreateInsert) {
        if ($Line -match 'CREATE TABLE') {
            $global:OutputState = [OutputFileState]::WaitingSeparator
        }
    }
    # Vai lendo o arquivo atual até achar a instrução 'ALTER TABLE', quando achar é o ponto que vai começar a escrever as referências (Foreign Keys, Index e etc)
    if ($global:OutputState -eq [OutputFileState]::WaitingSeparator -or $global:OutputState -eq [OutputFileState]::CloseIntermediaryFile) {
        if ($Line.StartsWith('ALTER TABLE')) {
            $global:OutputState = [OutputFileState]::Dependencies
        }
    }
    # Se o estado atual não for Dependencies então vai escrever os dados no arquivo DCI
    if ($global:OutputState -ne [OutputFileState]::Dependencies) {
        if ($null -eq $global:OutputWriter) {
            $OutputFile = GetOutputFileName -TableName $TableName -RefsPath $global:DCIPath
            if (Test-Path -Path $OutputFile) {
                $global:ShouldWriteFile = $false
            } else {
                $global:OutputWriter = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
            }
        }
        if ($global:ShouldWriteFile) {
            if ($global:CurrFileSize -eq 0 -and $global:CurrFileIndex -ne 0) {
                Write-Host "... Creating chunk $global:CurrFileIndex... " -ForegroundColor Yellow -NoNewline
            }
            $global:CurrFileSize += [System.Text.Encoding]::UTF8.GetByteCount($Line) + 2  # +2 for CRLF
            if ($global:CurrFileSize -gt $global:ChunckSize) {
                $global:OutputState = [OutputFileState]::CloseIntermediaryFile
            }
            if ($global:OutputState -eq [OutputFileState]::CloseIntermediaryFile -and $Line.StartsWith('INSERT ')) {
                CloseIntermediaryOutputFile -TableName $TableName
                $OutputFile = GetOutputFileName -TableName $TableName -RefsPath $global:DCIPath
                $global:OutputState = [OutputFileState]::WaitingSeparator
                $global:OutputWriter = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
                StartIntermediaryOutputFile -TableName $TableName
            }
            $global:OutputWriter.WriteLine($Line)
        }
    }
    # Se o estado atual for Dependencies então vai escrever as referências no arquivo Refs
    else {
        if (-not ($global:IsRefsInitiated)) {
            $global:IsRefsInitiated = $true
            $global:ShouldWriteFile = $true
            CloseOutputWriter
            $Line = "USE $global:DatabaseName`r`nGO`r`n$($Line)"
            $OutputFile = GetOutputFileName -TableName $TableName -RefsPath $global:RefsPath
            if (Test-Path -Path $OutputFile) {
                $global:ShouldWriteFile = $false
            } else {
                $global:OutputWriter = [System.IO.StreamWriter]::new($OutputFile, $false, [System.Text.Encoding]::UTF8)
            }
        }
        if ($global:ShouldWriteFile) {
            $global:OutputWriter.WriteLine($Line)
        }
    }
}

function StartIntermediaryOutputFile {
    param (
        [Parameter(Mandatory = $true)][string]$TableName
    )
    $global:OutputWriter.WriteLine("USE $global:DatabaseName`r`nGO")
    $global:OutputWriter.WriteLine("SET IDENTITY_INSERT [dbo].[$TableName] ON")
}

function CloseIntermediaryOutputFile {
    param (
        [Parameter(Mandatory = $true)][string]$TableName
    )
    $global:OutputWriter.WriteLine("SET IDENTITY_INSERT [dbo].[$TableName] OFF")
    $global:OutputWriter.WriteLine("GO")
    CloseOutputWriter
    $global:CurrFileSize = 0
}

function GetOutputFileName {
    param (
        [Parameter(Mandatory = $true)][string]$TableName,
        [Parameter(Mandatory = $true)][string]$RefsPath
    )
    $OutputFile = "$($RefsPath)$([System.IO.Path]::DirectorySeparatorChar)$($TableName)."
    if ($global:ChunckedTables.ContainsKey($TableName) -and $RefsPath -eq $global:DCIPath) {
        $global:CurrFileIndex++
        $OutputFile += "$($global:CurrFileIndex)."
    }
    return $OutputFile + "$($RefsPath).sql"
}

function ResetStates {
    $global:CurrentState = [TableSearchStates]::Searching
    $global:OutputState = [OutputFileState]::DropCreateInsert
    $global:IsRefsInitiated = $false
    $global:OutputWriter = $null
    $global:ShouldWriteFile = $true
    $global:CurrFileSize = 0
    $global:CurrFileIndex = 0
    $global:DatabaseName = $null
}

function CloseOutputWriter {
    if ($global:CurrFileIndex -gt 0) {
        Write-Host "Done! " -ForegroundColor Green
    }
    if ($null -ne $global:OutputWriter) {
        $global:OutputWriter.Close()
        $global:OutputWriter.Dispose()
        $global:OutputWriter = $null
    }
}

function FinishFileProcessing {
    CloseOutputWriter
}

function GenerateTablesOrderedList {
    param (
        [Parameter(Mandatory = $true)][System.Collections.ArrayList]$FileList
    )
    if ($FileList.Count -eq 0) {
        Write-Host "No files found."
    }
    else {
        foreach ($file in $FileList) {
            ResetStates
            $Reader = [System.IO.StreamReader]::new($file)

            # Faz a verificação se o arquivo será dividido em partes
            $FileSize = $Reader.BaseStream.Length
            $ChunckLength = 0
            $CurrentTableName = GetTableNameFromFileName -FileName $(Get-Item $file).Name
            if ($FileSize -gt $global:ChunckSize) {
                $ChunckLength = [math]::Round($FileSize / $global:ChunckSize, 0)
                $global:ChunckedTables.Add($CurrentTableName, $ChunckLength) | Out-Null
            }

            # Inicia a organização do arquivo
            $FoundReferece = $false
            if ($ChunckLength -gt 0) {
                Write-Host "Processing file for: $CurrentTableName. File is too big, splitting into parts of $([math]::Round($global:ChunckSize / (1024*1024), 0))MB." -ForegroundColor Yellow
            } else {
                Write-Host "Processing file for: $CurrentTableName"
            }
            while ($null -ne ($Line = $Reader.ReadLine())) {
                if ([string]::IsNullOrWhiteSpace($Line)) {
                    continue
                }
                ProcessTableReferenceState -Line $Line
                if ((-not ($FoundReferece)) -and ($global:CurrentState -eq [TableSearchStates]::TableFound)) {
                    $FoundReferece = $true
                }
                ProcessOutputFileState -Line $Line -TableName $CurrentTableName
            }
            $Reader.Close()
            FinishFileProcessing -CurrentTableName $CurrentTableName
            if (-not $FoundReferece) {
                AddTableReferenceControl -CurrentTableName $CurrentTableName
            }
        }
    }
}

function RunSQLFile {
    param (
        [Parameter(Mandatory=$true)][string]$Type,
        [Parameter(Mandatory=$true)][string]$File
    )
    if (-not (Test-Path -Path $File)) {
        return
    }

    Write-Host "Executing file: $File"

    if ("" -eq $global:login -or "" -eq $global:pass) {
        sqlcmd -S "$global:hostname,$global:port" -E -x -i "$File" >> "${Type}_queries.log"
    } else {
        sqlcmd -S "$global:hostname,$global:port" -U $global:login -P $global:pass -x -i "$File" >> "$($Type)$([System.IO.Path]::DirectorySeparatorChar)${Type}_queries.log"
    }
}

function ExecuteScripts {
    foreach ($Type in $global:ExecutionOrder) {
        if ($Type -eq $global:DCIPath) {
            for ($i = 0; $i -lt $global:ReferencedTablesList.Count; $i++) {
                $Tables = $global:ReferencedTablesList[$i]
                foreach ($Table in $Tables) {
                    if ($global:ChunckedTables.ContainsKey($Table)) {
                        for ($j = 1; $j -le $global:ChunckedTables[$Table]; $j++) {
                            $File = ".$([System.IO.Path]::DirectorySeparatorChar)$($global:DCIPath)$([System.IO.Path]::DirectorySeparatorChar)$($Table).$j.$($global:DCIPath).sql"
                            if (-not (Test-Path -Path $File)) {
                                break
                            }
                            RunSQLFile -Type $Type -File $File
                        }
                    } else {
                        $File = ".$([System.IO.Path]::DirectorySeparatorChar)$($global:DCIPath)$([System.IO.Path]::DirectorySeparatorChar)$($Table).$($global:DCIPath).sql"
                        RunSQLFile -Type $Type -File $File
                    }
                }
            }
            continue
        }
        if ($Type -eq $global:RefsPath) {
            for ($i = $global:ReferencedTablesList.Count - 1; $i -ge 0; $i--) {
                $Tables = $global:ReferencedTablesList[$i]
                foreach ($Table in $Tables) {
                    $File = ".$([System.IO.Path]::DirectorySeparatorChar)$($global:RefsPath)$([System.IO.Path]::DirectorySeparatorChar)$($Table).$($global:RefsPath).sql"
                    RunSQLFile -Type $Type -File $File
                }
            }
            continue
        }
        $Files = Get-ChildItem -Path $Type -File -Filter "*.$($Type).sql"
        foreach ($File in $Files) {
            $TableName = GetTableNameFromFileName -FileName $(Get-Item $File).Name
            RunSQLFile -Type $Type -TableName $TableName
        }
    }
}

function PrettyPrintTablesList {
    Write-Host ""
    Write-Host "Total Referenced Levels: $($global:ReferencedTablesList.Count)"
    $LevelCount = 0
    foreach ($Tables in $global:ReferencedTablesList) {
        Write-Host "Level ${LevelCount}:" -ForegroundColor Blue
        foreach ($Table in $Tables) {
            Write-Host "... $Table"
        }
        $LevelCount++
    }
}

function CreateFolderOrCleanIt {
    param (
        [Parameter(Mandatory = $true)][string]$Path
    )
    if (-not (Test-Path -Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    } else{
        Get-ChildItem -Path $Path -Recurse | Remove-Item -Force -Recurse
    }
}

function MoveFilesToFolders {
    param (
        [Parameter(Mandatory = $true)][string]$Filter
    )
    $Files = Get-ChildItem -Path "." -File -Filter "*.$($Filter).sql"
    foreach ($File in $Files) {
        $DestinationPath = "$($Filter)$([System.IO.Path]::DirectorySeparatorChar)"
        Move-Item -Path $File -Destination $DestinationPath
    }
}

function GenerateTablesLevels {
    foreach ($Table in $global:AllTables) {
        $CurrentTableLevel = GetTableReferenceLevel -Level 0 -TableName $Table
        AddReferenceOnLevel -TableName $Table -Level $CurrentTableLevel
    }
}

function CreateAllFoldersAndMoveFiles {
    foreach ($Type in $global:ExecutionOrder) {
        CreateFolderOrCleanIt -Path $Type
        MoveFilesToFolders -Filter $Type
    }
}

function ValidateHostname {
    param (
        [Parameter(Mandatory = $true)][string]$Hostname
    )
    if ([string]::IsNullOrWhiteSpace($Hostname)) {
        Write-Host "Hostname cannot be empty." -ForegroundColor Red
        exit 1
    }
    # Valida se o hostname é um endereço IP válido
    if (-not ($Hostname -match '^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')) {
        Write-Host "Invalid IP address format. Please enter a valid IP address." -ForegroundColor Red
        exit 1
    }
}

function ValidatePort {
    param (
        [Parameter(Mandatory = $true)][string]$Port
    )
    # Valida se a porta informada é um número válido
    if (-not ($Port -match '^\d+$') -or [int]$Port -lt 1 -or [int]$Port -gt 65535) {
        Write-Host "Invalid port number. Please enter a valid port number between 1 and 65535." -ForegroundColor Red
        exit 1
    }
}

function ValidatePassword {
    param (
        [Parameter(Mandatory = $true)][string]$Password
    )
    if ([string]::IsNullOrWhiteSpace($Password)) {
        Write-Host "Password cannot be empty." -ForegroundColor Red
        exit 1
    }
}

function GetLoginCredentials {
    # Pega os dados do hostname de conexão
    $global:hostname = Read-Host "... Server IP address/hostname (default is 127.0.0.1)"
    if ([string]::IsNullOrWhiteSpace($global:hostname)) {
        $global:hostname = "127.0.0.1"
    }
    ValidateHostname -Hostname $global:hostname

    # Pega os dados da porta de conexão
    $global:port = Read-Host "... Server port (default is 1433)"
    if ([string]::IsNullOrWhiteSpace($global:port)) {
        $global:port = "1433"
    }
    ValidatePort -Port $global:port

    # Pega os dados de login e senha do banco de dados
    $global:login = Read-Host "... Database login (leave blank for Windows Authentication)"
    if ([string]::IsNullOrWhiteSpace($global:login)) {
        Write-Host "... Using Windows Authentication." -ForegroundColor Yellow
        $global:login = ""
        $global:pass = ""
    } else {
        $global:pass = Read-Host "... Database password"
        ValidatePassword -Password $global:pass
    }
}

# Código de execução do script
Write-Host "Starting ScripterKing..." -ForegroundColor Cyan

$global:hostname = $Hostname

# Verifica se o hostname foi fornecido como parâmetro
if (-not [string]::IsNullOrWhiteSpace($global:hostname)) {
    # Se foi fornecido, pega as outras variáveis
    $global:port = $Port
    $global:login = $Login
    $global:pass = $Password
    ValidateHostname -Hostname $global:hostname
    ValidatePort -Port $global:port
    if (-not [string]::IsNullOrWhiteSpace($global:login)) {
        ValidatePassword -Password $global:pass
    }
} else {
    GetLoginCredentials
}

# Remove espaços em branco desnecessários
$global:hostname = $global:hostname.Trim()
$global:port = $global:port.Trim()
$global:login = $global:login.Trim()
$global:pass = $global:pass.Trim()

Write-Host ""
Write-Host "Running..." -ForegroundColor Green
CreateAllFoldersAndMoveFiles
$Time = [Diagnostics.Stopwatch]::StartNew()
GenerateTablesOrderedList -FileList @(GetTableSqlFiles)
GenerateTablesLevels
PrettyPrintTablesList
# $Time.Stop()
Write-Host ""
Write-Host "Schema reading time: $([math]::Round($Time.Elapsed.TotalSeconds, 2)) seconds" -ForegroundColor Green
Write-Host ""
ExecuteScripts
Write-Host "Total execution time: $([math]::Round($Time.Elapsed.TotalSeconds, 2)) seconds" -ForegroundColor Green