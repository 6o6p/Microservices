using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microservices.ExternalServices.Authorization;
using Microservices.ExternalServices.Billing;
using Microservices.ExternalServices.Billing.Types;
using Microservices.ExternalServices.CatDb;
using Microservices.ExternalServices.CatExchange;
using Microservices.ExternalServices.Database;
using Microservices.Types;

namespace Microservices
{
    public class CatEntity : Cat, IEntityWithId<Guid>
    {
        public Guid Id { get; set; }
        public Guid BreedId { get; set; }
        public Guid AddedBy { get; set; }
        public string Breed { get; set; }
        public string Name { get; set; }
        public byte[] CatPhoto { get; set; }
        public byte[] BreedPhoto { get; set; }
        public decimal Price { get; set; }
        public List<(DateTime Date, decimal Price)> Prices { get; set; }

        public CatEntity(Cat cat)
        {
            Id = cat.Id;
            BreedId = cat.BreedId;
            AddedBy = cat.AddedBy;
            Breed = cat.Breed;
            Name = cat.Name;
            CatPhoto = cat.CatPhoto;
            BreedPhoto = cat.BreedPhoto;
            Price = cat.Price;
            Prices = cat.Prices;
        }

        public Cat MakeCat()
        {
            return new Cat
            {
                Id = this.Id,
                BreedId = this.BreedId,
                AddedBy = this.AddedBy,
                Breed = this.Breed,
                Name = this.Name,
                CatPhoto = this.CatPhoto,
                BreedPhoto = this.BreedPhoto,
                Price = this.Price,
                Prices = this.Prices,
            };
        }
    }

    public class UserFavorites : IEntityWithId<Guid>
    {
        public Guid Id { get; set; }
        public HashSet<Guid> Favorites { get; set; }
    }

    public class CatShelterService : ICatShelterService
    {
        private readonly IDatabase _db;
        private readonly IAuthorizationService _authorizationService;
        private readonly IBillingService _billingService;
        private readonly ICatInfoService _catInfoService;
        private readonly ICatExchangeService _catExchangeService;

        public CatShelterService(
            IDatabase database,
            IAuthorizationService authorizationService,
            IBillingService billingService,
            ICatInfoService catInfoService,
            ICatExchangeService catExchangeService)
        {
            _db = database;
            _authorizationService = authorizationService;
            _billingService = billingService;
            _catInfoService = catInfoService;
            _catExchangeService = catExchangeService;
        }

        public async Task<List<Cat>> GetCatsAsync(string sessionId, int skip, int limit, CancellationToken cancellationToken)
        {
            
            var collection = _db.GetCollection<CatEntity, Guid>("Cats");

            var billing = await _billingService.GetProductsAsync(skip, limit, cancellationToken);

            var cats = new List<Cat>();
            foreach(var product in billing)
            {
                var f = await collection.FindAsync(product.Id, cancellationToken);
                cats.Add(f.MakeCat());
            }

            return cats;
        }

        public async Task AddCatToFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            var user = await _authorizationService.AuthorizeAsync(sessionId, cancellationToken);

            var favorites = _db.GetCollection<UserFavorites, Guid>("Favorites");

            var userFavorites = await favorites.FindAsync(user.UserId, cancellationToken);

            

            if (userFavorites == null)
            {
                userFavorites = new UserFavorites { Id = user.UserId, Favorites = new HashSet<Guid>() };
                await favorites.WriteAsync(userFavorites, cancellationToken);
            }

            userFavorites.Favorites.Add(catId);
        }

        public async Task<List<Cat>> GetFavouriteCatsAsync(string sessionId, CancellationToken cancellationToken)
        {
            var user = await _authorizationService.AuthorizeAsync(sessionId, cancellationToken);
            var favorites = _db.GetCollection<UserFavorites, Guid>("Favorites");

            var userFavorites = await favorites.FindAsync(user.UserId, cancellationToken);

            var collection = _db.GetCollection<CatEntity, Guid>("Cats");

            var cats = new List<Cat>();

            if (userFavorites != null && userFavorites.Favorites != null)
            {
                foreach (var id in userFavorites.Favorites)
                {
                    var f = await collection.FindAsync(id, cancellationToken);
                    cats.Add(f.MakeCat());
                }
            }

            return cats;
        }

        public Task DeleteCatFromFavouritesAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<Bill> BuyCatAsync(string sessionId, Guid catId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public async Task<Guid> AddCatAsync(string sessionId, AddCatRequest request, CancellationToken cancellationToken)
        {
            var user = await _authorizationService.AuthorizeAsync(sessionId, cancellationToken);

            var catInfo = await _catInfoService.FindByBreedNameAsync(request.Breed, cancellationToken);

            var priceInfo = await _catExchangeService.GetPriceInfoAsync(catInfo.BreedId, cancellationToken);

            var latestPrice = priceInfo.Prices != null && priceInfo.Prices.Count > 0
                ? priceInfo.Prices.Last()
                : new ExternalServices.CatExchange.Types.CatPriceInfo { Date = DateTime.Now, Price = 1000 };

            var cat = new Cat()
            {
                Id = Guid.NewGuid(),
                BreedId = catInfo.BreedId,
                AddedBy = user.UserId,
                Breed = request.Breed,
                Name = request.Name,
                CatPhoto = request.Photo,
                BreedPhoto = catInfo.Photo,
                Price = latestPrice.Price,
                Prices = priceInfo.Prices.Select(p => (p.Date, p.Price)).ToList()
            };

            var billingTask = _billingService.AddProductAsync(new Product { Id = cat.Id, BreedId = cat.BreedId }, cancellationToken);

            var cats = _db.GetCollection<CatEntity, Guid>("Cats");
            await cats.WriteAsync(new CatEntity(cat), cancellationToken);

            return cat.Id;
        }
    }
}