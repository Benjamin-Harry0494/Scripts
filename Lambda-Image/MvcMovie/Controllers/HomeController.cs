using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using MvcMovie.Models;

using HtmlAgilityPack;
using PuppeteerSharp;
using PuppeteerSharp.Input;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Net;
using System.Text;
using System.IO;

namespace MvcMovie.Controllers;

public class HomeController : Controller
{

    private static NavigationOptions navigationOptions = new NavigationOptions {WaitUntil = new WaitUntilNavigation[] { WaitUntilNavigation.Networkidle0}};
    private readonly ILogger<HomeController> _logger;

    public HomeController(ILogger<HomeController> logger)
    {
        _logger = logger;
    }

    private static async Task launchBrowser()
    {
        const string url = "https://kchbank.allocate-cloud.com/BankStaffPreProd/(S(cyrj20rp1vhjq2ifam0za4rg))/UserLogin.aspx";
        await new BrowserFetcher().DownloadAsync(BrowserFetcher.DefaultChromiumRevision);
        var browser = await Puppeteer.LaunchAsync(new LaunchOptions
        {
            Headless = false,
            DefaultViewport = null
        });

        var delay = 200;
        var page = await browser.NewPageAsync();
        page.Request += Page_Request;
        page.Response += Page_Response; 

        await page.GoToAsync(url);

        var usernameSelector = "#ctl00_content_login_UserName";
        var passwordSelector = "#ctl00_content_login_Password";
        var loginSelector = "#ctl00_content_login_LoginButton";

        await page.WaitForSelectorAsync(usernameSelector);
        await TypeFieldValue(page, usernameSelector, "TomG", delay);
        Console.WriteLine("Completed Username");
        await page.WaitForSelectorAsync(passwordSelector);
        await TypeFieldValue(page, passwordSelector, "Today2@TomG", delay);
        Console.WriteLine("Completed Password");

        await page.ClickAsync(loginSelector); 
        Console.WriteLine("Logging in...");


        await browser.CloseAsync();

    }

    private static void Page_Request(object sender, RequestEventArgs e)
    {
        Console.WriteLine(e.Request.ResourceType.ToString());
        Console.WriteLine(e.Request.Url);
    }
    private static async Task TypeFieldValue(IPage page, string fieldSelector, string value, int delay = 0)
    {
        await page.FocusAsync(fieldSelector);
        await page.TypeAsync(fieldSelector, value, new TypeOptions { Delay = delay });
        await page.Keyboard.PressAsync("Tab");
    }

    private static async void Page_Response(object sender, ResponseCreatedEventArgs e)
    {
        Console.WriteLine(e.Response.Status);
    }

    public IActionResult Index()
    {
        launchBrowser();

        return View();
    }

    public IActionResult Privacy()
    {
        return View();
    }

    [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
    public IActionResult Error()
    {
        return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
    }
}
