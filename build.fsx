// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#r "nuget: Fake.Api.GitHub, 6.1.4"
#r "nuget: Fake.BuildServer.GitHubActions, 6.1.4"
#r "nuget: Fake.Core.Target, 6.1.4"
#r "nuget: Fake.Core.Vault, 6.1.4"
#r "nuget: Fake.Core.Xml, 6.1.4"
#r "nuget: Fake.Core.ReleaseNotes, 6.1.4"
#r "nuget: Fake.DotNet.Cli, 6.1.4"
#r "nuget: Fake.DotNet.NuGet, 6.1.4"
#r "nuget: Fake.DotNet.Fsdocs, 6.1.4"
#r "nuget: Fake.IO.FileSystem, 6.1.4"
#r "nuget: Fake.Tools.Git, 6.1.4"
#r "nuget: MSBuild.StructuredLogger, 2.2.386" // MSBuild log version fix
#r "nuget: System.Formats.Asn1, 9.0.0" // vulnerabilities


open Fake.Core
open Fake.DotNet
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.IO.Globbing.Operators

// -------------------
//   Bootstrap Fake
// -------------------
#if !FAKE
// To run script without fake.exe (no need multiple .NET sdk versions) - https://fake.build/guide/fake-debugging.html#Run-script-without-fake-exe-via-fsi
Fake.Core.Context.setExecutionContextFromCommandLineArgs __SOURCE_FILE__
#endif

// --------------------------------------------------------------------------------------
// Provide project-specific details below
// --------------------------------------------------------------------------------------

[<AutoOpen>]
module Project =

    let root =__SOURCE_DIRECTORY__

    /// Git configuration (used for publishing documentation in gh-pages branch)
    /// The profile where the project is posted 
    let [<Literal>] gitHome = "https://github.com/p1eXu5"

    /// The name of the project on GitHub
    let gitName = "p1eXu5.FSharp.Reactive"

    let docsDir = root </>  "docs"
    let docsOutput     = docsDir </> "output"

    let ``p1eXu5.FSharp.Reactive`` =
        root </> "src/p1eXu5.FSharp.Reactive/p1eXu5.FSharp.Reactive.fsproj"

    let ``p1eXu5.FSharp.Reactive nuget`` version = $"p1eXu5.FSharp.Reactive.{version}.nupkg" 

    let ``p1eXu5.FSharp.Reactive.Testing`` =
        root </> "src/p1eXu5.FSharp.Reactive.Testing/p1eXu5.FSharp.Reactive.Testing.fsproj"

    let ``p1eXu5.FSharp.Reactive.Testing nuget`` version = $"p1eXu5.FSharp.Reactive.Testing.{version}.nupkg" 

    let tests =
        root </> "tests/p1eXu5.FSharp.Reactive.Tests/p1eXu5.FSharp.Reactive.Tests.fsproj"

    let all =
        !! ``p1eXu5.FSharp.Reactive``
        ++ ``p1eXu5.FSharp.Reactive.Testing``
        ++ tests

    let ``RELEASE_NOTES.md`` =
        lazy (
            ReleaseNotes.load (root </> "RELEASE_NOTES.md")
        )

    let ``TESTING_RELEASE_NOTES.md`` =
        lazy (
            ReleaseNotes.load (root </> "TESTING_RELEASE_NOTES.md")
        )

    let nugetsOutput = root </> "nugets"


module FakeVarKeys =
    let [<Literal>] ``p1eXu5.FSharp.Reactive version`` = "ReactiveVersionString"
    let [<Literal>] ``p1eXu5.FSharp.Reactive.Testing version`` = "ReactiveTestingVersionString"

// --------------------------------------------------------------------------------------
// Environment & Secrets
// --------------------------------------------------------------------------------------
module Secrets =
    let mutable secrets = []

    let vault = Vault.fromFakeEnvironmentVariable()

    let getFromVaultOrEnvOrDefault name defaultValue =
        match vault.TryGet name with
        | Some v -> v
        | None -> Environment.environVarOrDefault name defaultValue

    let releaseSecret replacement name =
        let secret =
            lazy
                let env =
                    match getFromVaultOrEnvOrDefault name "default_unset" with
                    | "default_unset" -> failwithf "variable '%s' is not set" name
                    | s -> s
                TraceSecrets.register replacement env
                env
        secrets <- secret :: secrets
        secret

    let [<Literal>] GITHUB_NUGET_SOURCE = "github"

    let githubReleaseUser = getFromVaultOrEnvOrDefault "GITHUB_ACTOR" "p1eXu5"
    let gitName = getFromVaultOrEnvOrDefault "REPOSITORY_NAME_GITHUB" "p1eXu5.FSharp.Reactive"

    let githubToken = releaseSecret "<githubtoken>" "GITHUB_TOKEN"
    // let nugetOrgToken = releaseSecret "<githubtoken>" "PUBLISH_TO_NUGET_ORG"


// --------------------------------------------------------------------------------------
// Check & preparing targets
// --------------------------------------------------------------------------------------

Target.create "CheckReleaseSecrets" (fun p ->
    for secret in Secrets.secrets do
        secret.Force() |> ignore
)

Target.create "GetVersion" (fun _ ->
    let versionString =
        Xml.read true Project.``p1eXu5.FSharp.Reactive`` "" "" "//Version"
        |> Seq.head

    let releaseNotes = Project.``RELEASE_NOTES.md``.Value

    if versionString <> releaseNotes.NugetVersion then
        failwith (sprintf "Release notes for version %s has not been found. Release notes: %A" versionString releaseNotes)

    Trace.log $"p1eXu5.FSharp.Reactive version string is: {versionString}"
    FakeVar.set FakeVarKeys.``p1eXu5.FSharp.Reactive version`` versionString

    let versionString =
        Xml.read true Project.``p1eXu5.FSharp.Reactive.Testing`` "" "" "//Version"
        |> Seq.head

    let releaseNotes = Project.``TESTING_RELEASE_NOTES.md``.Value

    if versionString <> releaseNotes.NugetVersion then
        failwith (sprintf "Release notes for version %s has not been found. Release notes: %A" versionString releaseNotes)

    Trace.log $"p1eXu5.FSharp.Reactive.Testing version string is: {versionString}"
    // Store the version string in the context for later use
    FakeVar.set FakeVarKeys.``p1eXu5.FSharp.Reactive.Testing version`` versionString
)


// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages
// --------------------------------------------------------------------------------------

Target.create "Clean" (fun _ ->
    !! "src/**/bin/Release"
    ++ "src/**/obj/Release"
    ++ "test/**/bin/Release"
    ++ "test/**/obj/Release"
    |> Shell.cleanDirs
)


Target.create "CleanDocs" (fun _ ->
    Shell.cleanDir docsOutput
)


Target.create "Restore" (fun _ ->
    !!("./**/*.*sproj")
    |> Seq.iter (DotNet.restore id)
)


Target.create "Build" (fun _ ->
    let optsf (opts: DotNet.BuildOptions) =
        {
            opts with
                Configuration = DotNet.BuildConfiguration.Release
                NoRestore = true
        }

    all
    |> Seq.iter (fun p ->
        DotNet.build optsf p
    )
)



Target.create "Test" (fun _ ->
    DotNet.test (fun opts ->
        { opts with
            Configuration = DotNet.BuildConfiguration.Release
            NoRestore = true
            NoBuild = true
        }) Project.tests
)


Target.create "Pack" (fun _ ->
    let optsf versionSuffix (packageReleaseNotes: ReleaseNotes.ReleaseNotes) (opts: DotNet.PackOptions) =
        {
            opts with
                Configuration = DotNet.BuildConfiguration.Release
                OutputPath = Some Project.nugetsOutput
                VersionSuffix = Some versionSuffix
                NoRestore = true
                NoBuild = true
                NoLogo = true
                MSBuildParams =
                    { opts.MSBuildParams with
                        Properties =
                            [
                                // Join notes with newline or literal \n for MSBuild
                                "PackageReleaseNotes", packageReleaseNotes.Notes |> String.concat "\n"
                            ]
                    }
        }

    let versionString = FakeVar.getOrFail FakeVarKeys.``p1eXu5.FSharp.Reactive version``
    let releaseNotes = Project.``RELEASE_NOTES.md``.Value

    Trace.log $"Publishing p1eXu5.FSharp.Reactive version is: {versionString}"
    DotNet.pack (optsf versionString releaseNotes) Project.``p1eXu5.FSharp.Reactive``

    let versionString = FakeVar.getOrFail FakeVarKeys.``p1eXu5.FSharp.Reactive.Testing version``
    let releaseNotes = Project.``TESTING_RELEASE_NOTES.md``.Value

    Trace.log $"Publishing p1eXu5.FSharp.Reactive.Testing version is: {versionString}"
    DotNet.pack (optsf versionString releaseNotes) Project.``p1eXu5.FSharp.Reactive.Testing``
)

// dotnet nuget add source --username p1eXu5 --password ${{ secrets.GITHUB_TOKEN }} --store-password-in-clear-text --name github "https://nuget.pkg.github.com/p1eXu5/index.json"
Target.create "AddGithubNugetSource" (fun _ ->
    let result =
        DotNet.exec id "nuget" (
            sprintf
                "add source --username %s --password %s --store-password-in-clear-text --name %s https://nuget.pkg.github.com/p1eXu5/index.json"
                Secrets.githubReleaseUser
                Secrets.githubToken.Value
                Secrets.GITHUB_NUGET_SOURCE
        )
    
    if not result.OK then
        failwithf "dotnet nuget failed with errors: %A" result.Errors
)

// Publish on nuget.org target
Target.create "PublishOnNugetOrg" (fun _ ->
    failwith "not implemented"
    //DotNet.nugetPush (fun opts ->
    //    { opts with
    //        PushParams =
    //            { opts.PushParams with
    //                ApiKey = Secrets.nugetOrgToken.Value |> Some
    //                Source = "https://api.nuget.org/v3/index.json" |> Some
    //            }
    //        Common =
    //            { opts.Common with
    //                WorkingDirectory = Project.nugetsFolderPath
    //            }
            
    //    }) Project.nugetPath
)

// Publish target
Target.create "PublishOnGithub" (fun _ ->
    let optsf (opts: DotNet.NuGetPushOptions) =
        { opts with
            PushParams =
                { opts.PushParams with
                    ApiKey = Secrets.githubToken.Value |> Some
                    Source = Secrets.GITHUB_NUGET_SOURCE |> Some
                }
            Common =
                { opts.Common with
                    WorkingDirectory = Project.nugetsOutput
                }
        }

    let versionString = FakeVar.getOrFail FakeVarKeys.``p1eXu5.FSharp.Reactive version``
    DotNet.nugetPush optsf (Project.``p1eXu5.FSharp.Reactive nuget`` versionString)

    let versionString = FakeVar.getOrFail FakeVarKeys.``p1eXu5.FSharp.Reactive.Testing version``
    DotNet.nugetPush optsf (Project.``p1eXu5.FSharp.Reactive.Testing nuget`` versionString)
)


// --------------------------------------------------------------------------------------
// Generate the documentation
// --------------------------------------------------------------------------------------

Target.create "GenerateDocs" (fun _ ->
    
    failwith "not implemented"

    let githubLink = "https://github.com/p1eXu5/p1eXu5.FSharp.Reactive"

    Fsdocs.build (fun s ->
        {
            s with
                Output = docsOutput |> Some
                // Source = content
                // OutputDirectory = output
                // Template = docTemplate
                // ProjectParameters = info
                // LayoutRoots = layoutRoots
        })
)

Target.create "HostDocs" (fun _ ->
    failwith "not implemented"
)

// --------------------------------------------------------------------------------------
// Release Scripts
// --------------------------------------------------------------------------------------

Target.create "ReleaseDocs" (fun _ ->
    failwith "not implemented"
    // let tempDocsDir = "temp/gh-pages"
    // Shell.cleanDir tempDocsDir
    // Git.Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir
    // 
    // Shell.copyRecursive docsOutput tempDocsDir true |> Trace.tracefn "%A"
    // Git.Staging.stageAll tempDocsDir
    // Git.Commit.exec tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    // Git.Branches.push tempDocsDir
)



// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override
// --------------------------------------------------------------------------------------

Target.create "All" (fun _ ->
    Target.listAvailable()
)

open Fake.Core.TargetOperators

"CleanDocs"
  ==> "GenerateDocs"
  ==> "ReleaseDocs"

"CheckReleaseSecrets"
    ==> "GetVersion"
    ==> "Clean"
    ==> "Restore"
    ==> "Build"
    ==> "Test"
    ==> "Pack"
    ==> "PublishOnNugetOrg"
    ==> "All"

let ctx = Target.WithContext.runOrDefaultWithArguments "All"
Target.updateBuildStatus ctx
Target.raiseIfError ctx // important to have proper exit code on build failures.
